##############################################################
# label_printer_app.py
# Streamlit app for warehouse team to manage labels
#
# pip install streamlit boto3 pypdf
# streamlit run label_printer_app.py
##############################################################

import streamlit as st
import boto3
import os
import io
import time
from datetime import datetime
from botocore.exceptions import ClientError
from PyPDF2 import PdfWriter, PdfReader

# --- Config ---
BUCKET_NAME = "vwslabels"
PROCESSED_PREFIX = "processed/"
PRINTED_PREFIX = "printed/"
ERRORS_PREFIX = "errors/"

# --- Page Config ---
st.set_page_config(
    page_title="VWS Label Printer",
    page_icon="🏷️",
    layout="wide",
)

# --- S3 Client ---
@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
        region_name=st.secrets["aws"]["region_name"],
    )

s3 = get_s3_client()


# =========================================================
# S3 Helpers
# =========================================================
def list_files(prefix):
    try:
        paginator = s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key != prefix and not key.endswith("/"):
                    files.append({
                        "key": key,
                        "filename": os.path.basename(key),
                        "size_kb": round(obj["Size"] / 1024, 1),
                        "last_modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
                    })
        return files
    except ClientError as e:
        st.error(f"Failed to list files: {e}")
        return []


def get_file_bytes(key):
    resp = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return resp["Body"].read()


def move_file(source_key, dest_prefix):
    filename = os.path.basename(source_key)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_key = f"{dest_prefix}{timestamp}_{filename}"
    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={"Bucket": BUCKET_NAME, "Key": source_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)


def move_files_bulk(files, dest_prefix):
    for f in files:
        move_file(f["key"], dest_prefix)


def delete_file(key):
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)


# =========================================================
# PDF Merge
# =========================================================
def merge_pdfs(files):
    """Download all PDFs from S3 and merge into a single PDF."""
    writer = PdfWriter()
    skipped = []

    for f in files:
        try:
            pdf_bytes = get_file_bytes(f["key"])
            reader = PdfReader(io.BytesIO(pdf_bytes))
            for page in reader.pages:
                writer.add_page(page)
        except Exception as e:
            skipped.append(f["filename"])

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output.getvalue(), skipped


# =========================================================
# UI
# =========================================================
st.title("🏷️ VWS Label Printer")

# Sidebar
with st.sidebar:
    st.header("📊 Dashboard")
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

    processed = list_files(PROCESSED_PREFIX)
    printed = list_files(PRINTED_PREFIX)
    errors = list_files(ERRORS_PREFIX)

    st.metric("Ready to Print", len(processed))
    st.metric("Printed", len(printed))
    st.metric("Errors", len(errors))

    st.divider()
    st.caption(f"Bucket: {BUCKET_NAME}")
    st.caption(f"Refreshed: {datetime.now().strftime('%H:%M:%S')}")

# Tabs
tab_print, tab_printed, tab_errors = st.tabs([
    f"🖨️ Ready to Print ({len(processed)})",
    f"✅ Printed ({len(printed)})",
    f"❌ Errors ({len(errors)})",
])

# ---------------------------------------------------------
# Tab 1: Ready to Print
# ---------------------------------------------------------
with tab_print:
    if not processed:
        st.info("No labels waiting to be printed.")
    else:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write(f"**{len(processed)} label(s)** ready to print")

        # Generate merged PDF
        with col2:
            if st.button("📄 Generate Print Batch", type="primary", use_container_width=True):
                with st.spinner("Merging PDFs..."):
                    merged_pdf, skipped = merge_pdfs(processed)

                st.session_state["merged_pdf"] = merged_pdf
                st.session_state["merged_files"] = processed
                st.session_state["skipped"] = skipped

        # Show download button if PDF is ready
        if "merged_pdf" in st.session_state:
            skipped = st.session_state.get("skipped", [])
            merged_files = st.session_state["merged_files"]

            if skipped:
                st.warning(f"Skipped {len(skipped)} non-PDF file(s): {', '.join(skipped)}")

            st.divider()

            dl_col, move_col = st.columns(2)
            with dl_col:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    "⬇️ Download Merged PDF",
                    data=st.session_state["merged_pdf"],
                    file_name=f"labels_batch_{timestamp}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            with move_col:
                if st.button(
                    "✅ Mark as Printed",
                    use_container_width=True,
                    help="Move all labels to printed/ folder"
                ):
                    with st.spinner("Moving files..."):
                        move_files_bulk(merged_files, PRINTED_PREFIX)
                    del st.session_state["merged_pdf"]
                    del st.session_state["merged_files"]
                    del st.session_state["skipped"]
                    st.success("All labels moved to printed.")
                    time.sleep(1)
                    st.rerun()

        # Individual file list
        st.divider()
        for f in processed:
            with st.container():
                col1, col2, col3 = st.columns([4, 3, 1])
                with col1:
                    st.write(f"📄 **{f['filename']}**")
                with col2:
                    st.caption(f"{f['size_kb']} KB · {f['last_modified']}")
                with col3:
                    pdf_bytes = get_file_bytes(f["key"])
                    st.download_button(
                        "⬇️",
                        data=pdf_bytes,
                        file_name=f["filename"],
                        mime="application/pdf",
                        key=f"dl_{f['key']}",
                    )

# ---------------------------------------------------------
# Tab 2: Printed (Archive)
# ---------------------------------------------------------
with tab_printed:
    if not printed:
        st.info("No printed labels yet.")
    else:
        st.write(f"**{len(printed)} label(s)** in archive")

        if st.button("🗑️ Clear Archive"):
            for f in printed:
                delete_file(f["key"])
            st.success("Archive cleared.")
            time.sleep(1)
            st.rerun()

        st.divider()
        for f in printed:
            with st.container():
                col1, col2, col3 = st.columns([4, 3, 1])
                with col1:
                    st.write(f"✅ **{f['filename']}**")
                with col2:
                    st.caption(f"{f['size_kb']} KB · {f['last_modified']}")
                with col3:
                    pdf_bytes = get_file_bytes(f["key"])
                    st.download_button(
                        "⬇️",
                        data=pdf_bytes,
                        file_name=f["filename"],
                        mime="application/pdf",
                        key=f"dl_printed_{f['key']}",
                    )

# ---------------------------------------------------------
# Tab 3: Errors
# ---------------------------------------------------------
with tab_errors:
    if not errors:
        st.success("No errors.")
    else:
        st.write(f"**{len(errors)} label(s)** failed to match")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Retry All"):
                for f in errors:
                    try:
                        filename = os.path.basename(f["key"])
                        s3.copy_object(
                            Bucket=BUCKET_NAME,
                            CopySource={"Bucket": BUCKET_NAME, "Key": f["key"]},
                            Key=f"incoming/{filename}",
                        )
                        s3.delete_object(Bucket=BUCKET_NAME, Key=f["key"])
                    except Exception as e:
                        st.error(f"Failed to retry {f['filename']}: {e}")
                st.success("All moved back to incoming/ for reprocessing.")
                time.sleep(1)
                st.rerun()
        with col2:
            if st.button("🗑️ Clear Errors"):
                for f in errors:
                    delete_file(f["key"])
                st.success("Errors cleared.")
                time.sleep(1)
                st.rerun()

        st.divider()
        for f in errors:
            with st.container():
                col1, col2, col3, col4 = st.columns([4, 3, 1, 1])
                with col1:
                    st.write(f"❌ **{f['filename']}**")
                with col2:
                    st.caption(f"{f['size_kb']} KB · {f['last_modified']}")
                with col3:
                    if st.button("🔄", key=f"retry_{f['key']}"):
                        try:
                            filename = os.path.basename(f["key"])
                            s3.copy_object(
                                Bucket=BUCKET_NAME,
                                CopySource={"Bucket": BUCKET_NAME, "Key": f["key"]},
                                Key=f"incoming/{filename}",
                            )
                            s3.delete_object(Bucket=BUCKET_NAME, Key=f["key"])
                            st.success(f"Retrying {filename}")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")
                with col4:
                    pdf_bytes = get_file_bytes(f["key"])
                    st.download_button(
                        "⬇️",
                        data=pdf_bytes,
                        file_name=f["filename"],
                        mime="application/pdf",
                        key=f"dl_error_{f['key']}",
                    )