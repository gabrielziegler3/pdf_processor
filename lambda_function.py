"""
User uploads file to bucket 'source_bucket'

'source_bucket' triggers an event to an SQS queue
-> The event contains the bucket name and the file name
-> The lambda consumes the event, downloads the file from the bucket, processes it, and uploads the result to 'output_bucket'
"""

import io
import boto3
import pathlib
import fitz
import json


s3_client = boto3.client("s3")


def process_pdf(
    pdf_path_or_stream: pathlib.Path | bytes,
) -> str | None:
    """
    Process a PDF file

    Args:
        pdf_path_or_stream (pathlib.Path | bytes): The path to the PDF file or a byte stream

    Returns:
        str | None: The extracted pages as a JSON string, or None if no data was extracted
    """
    extracted_pages = []
    pdf_path = None

    if isinstance(pdf_path_or_stream, bytes):
        pdf_context = fitz.open(stream=pdf_path_or_stream)
        print("Processing PDF from byte stream")
    elif isinstance(pdf_path_or_stream, pathlib.Path):
        pdf_context = fitz.open(pdf_path_or_stream)
        pdf_path = pdf_path_or_stream
        print(f"Processing PDF from file: {pdf_path}")
    else:
        raise ValueError(f"Invalid pdf_path_or_stream: {pdf_path_or_stream}")

    with pdf_context as doc:
        print(f"Document with {doc.page_count} pages")
        for page in doc:
            # Increment because fitz page numbers start at 0
            page_number = page.number + 1

            # some computational activity on the pages and append the processed data
            if page_number % 2 == 0:
                extracted_pages.append(page_number)

        return extracted_pages


def lambda_handler(event: dict, context: object) -> None:
    """
    Process an event from an SQS queue.

    Args:
        event (dict): The event to process
        context (object): The context object

    Returns:
        None
    """
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        filename = record["s3"]["object"]["key"]
        output_filename = pathlib.Path(filename).stem + ".json"

        print(f"Processing record with file {filename}")

        file_stream = io.BytesIO()
        s3_client.download_fileobj(bucket_name, filename, file_stream)

        pages = process_pdf(
            pdf_path_or_stream=file_stream.getvalue(),
        )

        if not pages:
            print(f"No data extracted for file {filename}")
            return

        try:
            s3_client.put_object(
                Bucket="output_bucket", Key=output_filename, Body=json.dumps(pages)
            )
        except Exception as e:
            print(f"Error uploading file {filename} to S3: {e}")

        print(
            f"Data extracted for file {filename} and uploaded to S3 as {output_filename}"
        )
