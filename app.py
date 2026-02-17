##############################################################
# FILE 1: lambda_function.py
# Deploy this as your Lambda function
# Trigger: SQS queue (NOT directly from S3)
# S3 event -> SQS queue -> Lambda (batch processing)
##############################################################

import requests
import boto3
import os
import logging
import time
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Environment Variables (set in Lambda config) ---
USERNAME = os.environ.get("VERACORE_USERNAME")
PASSWORD = os.environ.get("VERACORE_PASSWORD")
SYSTEM_ID = os.environ.get("VERACORE_SYSTEM_ID")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

# --- Constants ---
BUCKET_NAME = "vwslabels"
INCOMING_PREFIX = "incoming/"
PROCESSED_PREFIX = "processed/"
ERRORS_PREFIX = "errors/"
VERACORE_BASE_URL = "https://wms.3plwinner.com/VeraCore/Public.Api/api"

# Update these to match your VeraCore report
ORDER_REPORT_NAME = "Open Orders"  # <-- Your report name
ORDER_ID_COLUMN = "OrderID"        # <-- Column name in report

s3 = boto3.client("s3")
sns = boto3.client("sns")


# =========================================================
# Lambda Handler - Processes SQS batch
# =========================================================
def handler(event, context):
    """
    Triggered by SQS. Receives a batch of S3 event notifications.
    Authenticates once, pulls report once, matches all labels.
    """
    # Step 1: Parse all S3 file references from the SQS batch
    files = parse_sqs_records(event)
    if not files:
        logger.info("No valid files in batch.")
        return {"statusCode": 200, "body": "No files"}

    logger.info(f"Processing batch of {len(files)} label files")

    # Step 2: Authenticate with VeraCore (once for entire batch)
    auth_header = get_token()
    if not auth_header:
        send_alert(
            "Label Matcher - Authentication Failure",
            f"Failed to authenticate with VeraCore.\n\n"
            f"{len(files)} label file(s) could not be processed and have been moved to errors/:\n\n"
            + "\n".join([f"- {f['filename']}" for f in files])
        )
        for f in files:
            move_file(f["bucket"], f["key"], ERRORS_PREFIX, f["filename"])
        return {"statusCode": 500, "body": "Auth failed"}

    # Step 3: Pull report ONCE for the entire batch
    report_data = pull_report(auth_header)
    if report_data is None:
        send_alert(
            "Label Matcher - Report Failure",
            f"Failed to pull VeraCore report '{ORDER_REPORT_NAME}'.\n\n"
            f"{len(files)} label file(s) could not be processed and have been moved to errors/:\n\n"
            + "\n".join([f"- {f['filename']}" for f in files])
        )
        for f in files:
            move_file(f["bucket"], f["key"], ERRORS_PREFIX, f["filename"])
        return {"statusCode": 500, "body": "Report failed"}

    # Step 4: Build a lookup set from report data for fast matching
    order_ids = {
        str(row.get(ORDER_ID_COLUMN, "")).strip()
        for row in report_data
        if row.get(ORDER_ID_COLUMN)
    }
    logger.info(f"Report returned {len(report_data)} rows, {len(order_ids)} unique order IDs")

    # Step 5: Match each label file
    matched = []
    unmatched = []

    for f in files:
        order_ref = f["order_ref"]
        if order_ref in order_ids:
            move_file(f["bucket"], f["key"], PROCESSED_PREFIX, f["filename"])
            matched.append(f["filename"])
        else:
            move_file(f["bucket"], f["key"], ERRORS_PREFIX, f["filename"])
            unmatched.append(f["filename"])

    logger.info(f"Matched: {len(matched)}, Unmatched: {len(unmatched)}")

    # Step 6: Send alerts
    if unmatched:
        send_alert(
            f"Label Matcher - {len(unmatched)} Unmatched Label(s)",
            f"The following label files were uploaded but no matching order "
            f"was found in VeraCore report '{ORDER_REPORT_NAME}':\n\n"
            + "\n".join([f"- {f}" for f in unmatched])
            + "\n\nThese files have been moved to the errors/ folder.\n"
            "Please verify filenames match existing order references."
        )

    if matched:
        send_alert(
            f"Label Matcher - {len(matched)} Label(s) Matched Successfully",
            f"The following labels were matched to orders and are ready for printing:\n\n"
            + "\n".join([f"- {f}" for f in matched])
        )

    return {
        "statusCode": 200,
        "body": json.dumps({"matched": len(matched), "unmatched": len(unmatched)})
    }


# =========================================================
# SQS Record Parsing
# =========================================================
def parse_sqs_records(event):
    """Extract S3 file info from SQS messages (which wrap S3 events)."""
    files = []
    for record in event.get("Records", []):
        try:
            body = json.loads(record["body"])
            # S3 notifications can be wrapped in SNS or sent directly
            s3_records = body.get("Records", [])
            for s3_rec in s3_records:
                bucket = s3_rec["s3"]["bucket"]["name"]
                key = s3_rec["s3"]["object"]["key"]
                filename = os.path.basename(key)

                # Skip folder markers, hidden files, non-PDFs
                if not filename or filename.startswith("."):
                    continue

                order_ref = os.path.splitext(filename)[0].strip()
                if not order_ref:
                    continue

                files.append({
                    "bucket": bucket,
                    "key": key,
                    "filename": filename,
                    "order_ref": order_ref,
                })
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse SQS record: {e}")
            continue
    return files


# =========================================================
# S3 Helpers
# =========================================================
def move_file(bucket, source_key, dest_prefix, filename):
    dest_key = f"{dest_prefix}{filename}"
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=dest_key,
        )
        s3.delete_object(Bucket=bucket, Key=source_key)
        logger.info(f"Moved {filename} -> {dest_prefix}")
    except Exception as e:
        logger.error(f"Failed to move {filename}: {e}")
        raise  # Let it fail loudly so SQS can retry


# =========================================================
# SNS Alerts
# =========================================================
def send_alert(subject, body):
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=body,
        )
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


# =========================================================
# VeraCore Authentication
# =========================================================
def get_token():
    body = {
        "userName": USERNAME,
        "password": PASSWORD,
        "systemId": SYSTEM_ID,
    }
    try:
        resp = requests.post(
            f"{VERACORE_BASE_URL}/Login", json=body, timeout=30
        )
        if resp.status_code == 200:
            token = resp.json().get("Token")
            if token:
                return {"Authorization": f"bearer {token}"}
        logger.error(f"Login failed: {resp.status_code} {resp.text[:300]}")
        return None
    except Exception as e:
        logger.error(f"Auth exception: {e}")
        return None


# =========================================================
# VeraCore Report
# =========================================================
def pull_report(auth_header):
    """Start report, poll until done, return data list."""
    # Start
    url = f"{VERACORE_BASE_URL}/reports"
    payload = {"reportName": ORDER_REPORT_NAME, "filters": []}
    try:
        resp = requests.post(url, json=payload, headers=auth_header, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Report start failed: {resp.status_code} {resp.text[:300]}")
            return None
        task_id = resp.json().get("TaskId")
        if not task_id:
            logger.error("No TaskId in report response")
            return None
    except Exception as e:
        logger.error(f"Report start exception: {e}")
        return None

    # Poll
    status_url = f"{VERACORE_BASE_URL}/reports/{task_id}/status"
    for attempt in range(30):
        try:
            resp = requests.get(status_url, headers=auth_header, timeout=30)
            if resp.status_code == 200:
                status = resp.json().get("Status")
                if status == "Done":
                    break
                elif status == "Request too Large":
                    logger.error("Report too large")
                    return None
                time.sleep(2)
            else:
                logger.error(f"Status check failed: {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"Poll exception: {e}")
            return None
    else:
        logger.error("Report timed out after 60s")
        return None

    # Fetch
    try:
        resp = requests.get(
            f"{VERACORE_BASE_URL}/reports/{task_id}",
            headers=auth_header, timeout=90
        )
        if resp.status_code == 200:
            return resp.json().get("Data", [])
        logger.error(f"Report fetch failed: {resp.status_code}")
        return None
    except Exception as e:
        logger.error(f"Report fetch exception: {e}")
        return None