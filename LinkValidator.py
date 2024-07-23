from typing import Tuple
import pandas as pd
import requests
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, PageBreak
from reportlab.lib.units import inch
from reportlab.lib import colors
import urllib3

# Suppress only the single InsecureRequestWarning from urllib3 needed to remove the SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_dataframe(file_path: str) -> pd.DataFrame:
    """Load a DataFrame from a given file path (CSV or XLSX)."""
    if file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file type. Please use a CSV or XLSX file.")
    return df

def save_dataframe(df: pd.DataFrame, file_path: str):
    """Save a DataFrame to a specified file path (CSV or XLSX)."""
    if file_path.endswith('.xlsx'):
        df.to_excel(file_path, index=False)
    elif file_path.endswith('.csv'):
        df.to_csv(file_path, index=False)
    else:
        raise ValueError("Unsupported file type. Please use a CSV or XLSX file.")

def update_links_to_https(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Update http links to https in a specified column of a DataFrame."""
    counter = 0
    for index, link in df[column_name].items():
        if isinstance(link, str):
            if link.startswith('//'):
                updated_link = 'https:' + link
                df.at[index, column_name] = updated_link
                counter += 1
                print(f"Row {index + 1}: {link} -> {updated_link}")
            if link.startswith('http://'):
                updated_link = link.replace('http://', 'https://')
                df.at[index, column_name] = updated_link
                counter += 1
                print(f"Row {index + 1}: {link} -> {updated_link}")
    print(f"Total links updated from http to https: {counter}")
    return df

def read_links(df: pd.DataFrame, column_name: str) -> Tuple[list, pd.DataFrame]:
    """Read and return a list of links from a specified column of a DataFrame."""
    links = df[column_name].dropna().tolist()
    print(f"Total links read from column: {len(links)}")
    return links, df

def check_link_status(link, index, verified_links, status_codes, df, column_name, test_websites):
    """Check the status of a given link and update the DataFrame accordingly."""
    if link in verified_links and verified_links[link] == "Working":
        return "Working", 200
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com'
    }
    try:
        response = requests.get(link, headers=headers, allow_redirects=True, timeout=20, verify=False)
        status_code = response.status_code
        status_codes[status_code] = status_codes.get(status_code, 0) + 1
        if status_code == 200:
            verified_links[link] = "Working"
            return "Working", status_code
        elif status_code == 406:
            print(f"Row {index + 1}: Non-200 status code for {link}: {response.status_code} - Not Acceptable")
            verified_links[link] = "Not Working"
            df.at[index, column_name] = ""
            return "Not Working", status_code
        elif status_code == 404:
            shortened_link = shorten_url(link)
            if shortened_link == link or not test_websites:
                print(f"Row {index + 1}: Non-200 status code for {link}: {response.status_code} - Not Found")
                df.at[index, column_name] = ""
                verified_links[link] = "Not Working"
                return "Not Working", status_code
            else:
                return check_link_status(shortened_link, index, verified_links, status_codes, df, column_name, test_websites)
        elif status_code == 429:
            verified_links[link] = "Working"
            return "Working", response.status_code
        elif status_code == 403:
            if test_websites:
                verified_links[link] = "Working"
                return "Working", status_code
            else:
                verified_links[link] = "Not Working"
                df.at[index, column_name] = ""
                return "Not Working", status_code
        else:
            verified_links[link] = "Not Working"
            df.at[index, column_name] = ""
            return "Not Working", status_code
    except requests.exceptions.RequestException as e:
        print(f"Row {index + 1}: Error checking {link}: {e}")
        status_codes[0] = status_codes.get(0, 0) + 1
        df.at[index, column_name] = ""
        verified_links[link] = "Not Working"
        return "Not Working", 0

def remove_igshid_parameter(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Remove the 'igshid' parameter from Instagram links in a specified column of a DataFrame."""
    counter = 0
    for index, link in df[column_name].items():
        if isinstance(link, str):
            parsed_url = urlparse(link)
            query_params = parse_qs(parsed_url.query)
            if 'igshid' in query_params:
                del query_params['igshid']
                new_query = urlencode(query_params, doseq=True)
                new_url = urlunparse(parsed_url._replace(query=new_query))
                df.at[index, column_name] = new_url
                counter += 1
                print(f"Row {index + 1}: {link} -> {new_url}")
    print(f"Total links with igshid parameter removed: {counter}")
    return df

def shorten_url(link):
    """Shorten a URL to its base URL."""
    parsed_url = urlparse(link)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
    return base_url

def generate_pdf_report(file_path, sheet_name, column_name, working_count, not_working_count, removed_count, total_count, status_codes, detailed_results):
    """Generate a PDF report of the link checking results."""
    # Define custom colors
    primary_color = colors.Color(103/255, 200/255, 117/255)
    secondary_color = colors.Color(35/255, 62/255, 65/255)

    datetime_title = datetime.now().strftime("%m-%d_%H:%M")

    doc = SimpleDocTemplate(f'{column_name}_report_{datetime_title}.pdf', pagesize=letter)
    styles = getSampleStyleSheet()

    # Create custom styles
    title_style = ParagraphStyle('CustomTitle', parent=styles['Title'], textColor=secondary_color, spaceAfter=20)
    heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'], textColor=secondary_color, spaceBefore=15, spaceAfter=10)
    body_style = ParagraphStyle('CustomBody', parent=styles['Normal'], textColor=secondary_color)
    date_style = ParagraphStyle('DateStyle', parent=styles['Normal'], textColor=secondary_color, alignment=1, fontSize=10)
    cell_style = ParagraphStyle('CellStyle', parent=styles['Normal'], textColor=secondary_color, wordWrap='CJK')

    elements = []

    # Add date and time
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_paragraph = Paragraph(f"Report generated on: {current_datetime}", date_style)
    elements.append(date_paragraph)
    elements.append(Paragraph("<br/>", body_style))

    # Title
    title = Paragraph(f'Link Status Report for {column_name}', title_style)
    elements.append(title)

    # Summary
    summary_data = [
        ('Total links in the column', total_count),
        ('Total links processed', working_count+not_working_count),
        ('Working links', working_count),
        ('Not working links', not_working_count),
        ('Links with igshid parameter removed', removed_count)
    ]
    summary_table = Table(summary_data, colWidths=[3*inch, 1.5*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), primary_color),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('LEFTPADDING', (0, 0), (-1, -1), 15),
        ('RIGHTPADDING', (0, 0), (-1, -1), 15),
    ]))
    elements.append(summary_table)
    elements.append(Paragraph('<br/><br/>', body_style))

    # Status Code Summary
    elements.append(Paragraph('Status Code Summary', heading_style))
    data = [['Status Code', 'Count']] + [[code, count] for code, count in status_codes.items()]
    table = Table(data, colWidths=[2*inch, 2*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), secondary_color),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('TEXTCOLOR', (0, 1), (-1, -1), secondary_color),
        ('GRID', (0, 0), (-1, -1), 1, primary_color),
        ('WORDWRAP', (0, 0), (-1, -1), 'CJK')  # Enable word wrap
    ]))
    elements.append(table)

    elements.append(PageBreak())

    # Detailed Link Status
    elements.append(Paragraph('Detailed Link Status', heading_style))
    data = [['Link', 'Status', 'Status Code']] + [[Paragraph(link, cell_style), status, status_code] for link, status, status_code in detailed_results]
    table = Table(data, colWidths=[4*inch, 1*inch, 1*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), secondary_color),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('TEXTCOLOR', (0, 1), (-1, -1), secondary_color),
        ('GRID', (0, 0), (-1, -1), 1, primary_color),
        ('WORDWRAP', (0, 0), (-1, -1), 'CJK')  # Enable word wrap
    ]))
    elements.append(table)

    doc.build(elements)

file_path = 'POI DB links.xlsx'
sheet_name = 'Copy of POI DB - POI_Data_'
column_name = 'Website'
test_websites = True

datetime_title = datetime.now().strftime("%m-%d_%H:%M")

json_file = f'verified_{column_name}_links_{datetime_title}.json'

# Load previously verified links
if os.path.exists(json_file):
    with open(json_file, 'r') as file:
        verified_links = json.load(file)
else:
    verified_links = {}

print("\n--------- Step 1: Update http to https in the file ---------\n")
df = load_dataframe(file_path)
df = update_links_to_https(df, column_name)
save_dataframe(df, file_path)
print(f"Updated links in {column_name}")

print("\n--------- Step 2: Remove igshid parameter from Instagram links ---------\n")
df = remove_igshid_parameter(df, column_name)
save_dataframe(df, file_path)
print(f"Removed igshid parameters from {column_name}")

print("\n--------- Step 3: Read links from the updated file ---------\n")
links, df = read_links(df, column_name)

print("\n--------- Step 4: Check the status of each link using threading ---------\n")
working_count = 0
not_working_count = 0
removed_count = df[column_name].str.contains('igshid').sum()
status_codes = {}
detailed_results = []

def process_link(index, link):
    """Process each link to check its status and update results."""
    try:
        status, status_code = check_link_status(link, index, verified_links, status_codes, df, column_name, test_websites)
    except Exception as e:
        status, status_code = "Error", 0
        print(f"Row {index + 1}: Error processing link {link}: {e}")
    detailed_results.append([link, status, status_code])
    return link, status

with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_link = {executor.submit(process_link, df.index[df[column_name] == link].tolist()[0], link): link for link in links}
    for future in as_completed(future_to_link):
        try:
            link, status = future.result()
        except Exception as e:
            link, status = "Unknown", "Error"
            print(f"Error in future result: {e}")
        if status == 'Working':
            working_count += 1
        else:
            not_working_count += 1

print("\n--------- Results ---------\n")

print(f"Total links checked: {len(links)}")
print(f"Working links: {working_count}")
print(f"Not working links: {not_working_count}")

# Save the verified links to JSON file
with open(json_file, 'w') as file:
    json.dump(verified_links, file)

# Save the updated DataFrame back to the file
save_dataframe(df, file_path)
print(f"Updated file saved at {file_path}")

# Generate PDF report
generate_pdf_report(file_path, sheet_name, column_name, working_count, not_working_count, removed_count, len(links), status_codes, detailed_results)

print("PDF report generated successfully.")
