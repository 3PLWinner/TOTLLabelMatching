# Label to Order Matching System (Vista Wine Spirits)

# S3 Bucket - vwslabels
    errors/
    incoming/
    processed/
    
## Workflow
1) Generate & pull unshipped orders dynamic report via API
2) List files in S3 incoming folder --> Lambda triggers parsing of filename to extract order ID
3) For each order, look for a matching filename
4) If match, pull label, print, move file to "processed" subfolder
5) If no match --> log/alert