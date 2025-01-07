# This is extract.py
import pdfplumber
import pandas as pd
import os
import sqlite3


def extract_data_from_pdf(pdf_path):
    """
    Extracts text and table data from a given PDF file.
    Args:
        pdf_path (str): Path to the PDF file.
    Returns:
        pd.DataFrame: Combined DataFrame with text and table data.
    """
    # Load the PDF
    pdf_pages = pdfplumber.open(pdf_path)

    page = pdf_pages.pages[0]
    text = page.extract_text()
    text_list = text.split("\n")

    # Create a dictionary with the extracted text data
    invoice_text_data = {
        "invoice_number": text_list[8].replace("Invoice Number: ", ""),
        "invoice_date": text_list[9].replace("Invoice Date: ", ""),
        "due_date": text_list[10].replace("Due Date: ", ""),
        "client_name": text_list[6],
        "payment_terms": text_list[11],
        "client_address": text_list[7],
    }

    tables = page.extract_tables()

    invoice_table_data = {
        "description": [x[0] for x in tables[0][1:]],
        "unit_price": [float(x[1].replace("$", "")) for x in tables[0][1:]],
        "quantity": [int(x[2]) for x in tables[0][1:]],
    }

    # Convert text data and table data to DataFrames
    invoice_text_df = pd.DataFrame([invoice_text_data])
    invoice_table_df = pd.DataFrame(invoice_table_data)

    # Duplicate text data for each row in the table and combine
    invoice_data = pd.concat([invoice_text_df] * len(invoice_table_df), ignore_index=True)
    invoice_data = pd.concat([invoice_data, invoice_table_df], axis=1)

    return invoice_data

def extract_and_transform_data():
    """
    Extracts and transforms data from all PDF files in the raw data folder.
    Returns:
        pd.DataFrame: Combined DataFrame for all invoices.
    """
    # List of files in the raw data folder
    pdf_files = os.listdir("data/raw")

    data = pd.DataFrame()

    # Iterate over all the PDFs
    for pdf in pdf_files:
        invoice_data = extract_data_from_pdf("data/raw/{}".format(pdf))
        data = pd.concat([data, invoice_data], ignore_index=True)

    return data

def store_data(data):
    """
    Stores the extracted data in multiple formats: CSV, Excel, and SQLite.
    Args:
        data (pd.DataFrame): The combined DataFrame to store.
    """
    if not os.path.exists("data/processed"):
        os.makedirs("data/processed")

    data.to_csv("data/processed/invoice_data.csv", index=False)
    data.to_excel("data/processed/invoice_data.xlsx", index=False)

    conn = sqlite3.connect("data/processed/invoice_data.db")
    data.to_sql("invoices", conn, if_exists="replace", index=False)
    conn.close()

    print("Data extraction, transformation, and storage complete.")


if __name__ == "__main__":
    data = extract_and_transform_data()
    store_data(data)