import re
from pandas import DataFrame,set_option,to_datetime,read_csv,options
from edi_codes import *
from datetime import date
from pyodbc import Connection,connect,Cursor
from os import system
from warnings import filterwarnings

filterwarnings('ignore')
options.display.float_format = "{:,.4f}".format
set_option('display.max_rows', None)
set_option('display.max_columns', None)

server:str = 'apu'
database:str = 'source_MitchellGrocery_2023'
connectionString:str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Integrated Security={True};Autocommit={True};Trusted_Connection=yes;'
conn:Connection = connect(connectionString)
cursor_1:Cursor = conn.cursor()
server_2:str = 'barney'
database_2:str = 'sandbox_mp'
connectionString_2:str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Integrated Security={True};Autocommit={True};Trusted_Connection=yes;'
conn_2:Connection = connect(connectionString)
cursor_2:Cursor = conn.cursor()

def invoice_identification_invoice_date(line:str) -> date:
    """
        Extracting the Invoice date from the line that
        has the prefix G01. This is supposed to be formatted
        such that the date is the 6 digits in positions 18-23 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        invoice_datetime = to_datetime(line[17:23],format='%y%m%d')
        return date(invoice_datetime.year,invoice_datetime.month,invoice_datetime.day)
    except:
        return ""

def invoice_identification_invoice_number(line:str) -> str:
    """
        Extracting the Invoice number from the line that
        has the prefix G01. This is supposed to be formatted
        such that the number is the 22 digits in positions 24-45 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    invoice_number:str = line[23:45]
    if(invoice_number.isspace()):
        return ""
    if(re.search(r"[^ ]+",invoice_number)):
        return re.search(r"[^ ]+",invoice_number).group().replace(',','')
    return ""

def invoice_identification_purchase_order_date(line:str) -> date:
    """
        Extracting the Invoice date from the line that
        has the prefix G01. This is supposed to be formatted
        such that the date is the 6 digits in positions 46-51 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        purchase_order_datetime = to_datetime(line[45:51],format='%y%m%d')
        return date(purchase_order_datetime.year,purchase_order_datetime.month,purchase_order_datetime.day)
    except:
        return ""

def invoice_identification_purchase_order_number(line:str) -> str:
    """
        Extracting the Invoice number from the line that
        has the prefix G01. This is supposed to be formatted
        such that the number is the 22 digits in positions 52-73 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    purchase_order_number:str = line[51:73]
    if(purchase_order_number.isspace()):
        return ""
    if(re.search(r"[^ ]+",purchase_order_number)):
        return re.search(r"[^ ]+",purchase_order_number).group().replace(',','')
    return ""

def reference_number_reference_number_qualifier_code(line:str) -> str:
    """
        Extracting the reference number qualifier code from the line that
        has the prefix N9. This is supposed to be formatted
        such that the number is the 2 digits in positions 18-19
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    return line[17:19]

def reference_number_reference_number_qualifier_code_description(line:str) -> str:
    """
        Extracting the reference number qualifier code description from the line that
        has the prefix N9. This is done by collecting the code from position 18-19
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    reference_number_qualifier_code:str = reference_number_reference_number_qualifier_code(line)
    try:
        reference_number_qualifier_code_description:str = n9_reference_identification_qualifier_codes[reference_number_qualifier_code]
        return reference_number_qualifier_code_description.replace(',','')
    except:
        return ""

def reference_number_reference_number(line:str) -> str:
    """
        Extracting the reference number from the line that
        has the prefix N9. This is supposed to be formatted
        such that the number is the 30 digits in positions 20-49
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    reference_number:str = line[19:49]
    if(reference_number.isspace()):
        return ""
    if(re.search(r".+",reference_number)):
        return re.search(r".+",reference_number).group().replace(',','')
    return "" 

def reference_number_free_form_description(line:str) -> str:
    """
        Extracting the reference number free form description from the line that
        has the prefix N9. This is supposed to be formatted
        such that the description is placed in positions 50-94
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    free_form_description:str = line[49:94]
    if(free_form_description.isspace()):
        return ""
    return re.search(r"[A-Za-z0-9]+.+",free_form_description).group().replace(',','')

def date_time_date_qualifier(line:str) -> str:
    """
        Extracting the date qualifier from the line that
        has the prefix G62. This is supposed to be formatted
        such that the number is the 2 digits in positions 18-19
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    date_qualifier:str = line[17:19]
    if(date_qualifier.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",date_qualifier)):
        return date_qualifier.replace(',','')
    return ""

def date_time_date_qualifier_description(line:str) -> str:
    """
        Extracting the reference number qualifier code description from the line that
        has the prefix N9. This is done by collecting the code from position 18-19
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    date_qualifier_code:str = date_time_date_qualifier(line)
    try:
        date_qualifier_code_description:str = g62_date_qualifier_codes[date_qualifier_code]
        return date_qualifier_code_description.replace(',','')
    except:
        return ""

def date_time_date_yymmdd(line:str) -> str:
    """
        Extracting the date from the line that
        has the prefix G62. This is supposed to be formatted
        such that the date is the 6 digits in positions 20-25
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        invoice_datetime = to_datetime(line[19:25],format='%y%m%d')
        return date(invoice_datetime.year,invoice_datetime.month,invoice_datetime.day)
    except:
        return ""

def note_special_instruction_reference_code(line:str) -> str:
    """
        Extracting the note reference code from the line that
        has the prefix NTE. This is supposed to be formatted
        such that the number is the 3 digits in positions 18-20
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    reference_code:str = line[17:20]
    if(reference_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",reference_code)):
        return re.search(r"[A-Za-z0-9]+",reference_code).group().replace(',','')
    return ""

def note_special_instruction_reference_code_description(line:str) -> str:
    """
        Extracting the reference number qualifier code description from the line that
        has the prefix NTE. This is done by collecting the code from position 18-20
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    note_reference_code:str = note_special_instruction_reference_code(line)
    try:
        note_reference_code_description:str = nte_note_reference_codes[note_reference_code]
        return note_reference_code_description.replace(',','')
    except:
        return ""

def note_special_instruction_free_form_description(line:str) -> str:
    """
        Extracting the reference number free form description from the line that
        has the prefix N9. This is supposed to be formatted
        such that the description is placed in positions 21-80
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    free_form_description:str = line[20:80]
    if(free_form_description.isspace()):
        return ""
    return re.search(r"[A-Za-z0-9]+.+",free_form_description).group().replace(',','')

def carrier_detail_transportation_method_type_code(line:str) -> str:
    """
        Extracting the transportation code from the line that
        has the prefix G27. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 18-19
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    transportation_method_type_code:str = line[17:19]
    if(transportation_method_type_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",transportation_method_type_code)):
        return re.search(r"[A-Za-z0-9]+",transportation_method_type_code).group().replace(',','')
    return ""

def carrier_detail_transportation_method_type_code_description(line:str) -> str:
    """
        Extracting the transportation code description from the line that
        has the prefix G27. This is done by collecting the code from position 18-19
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    transportation_method_type_code:str = carrier_detail_transportation_method_type_code(line)
    try:
        transportation_method_type_code_description:str = g27_carrier_detail_transportation_method_type_codes[transportation_method_type_code]
        return transportation_method_type_code_description.replace(',','')
    except:
        return ""

def carrier_detail_equipment_number(line:str) -> str:
    """
        Extracting the equipment number from the line that
        has the prefix G27. This is supposed to be formatted
        such that the number is the 1-10 characters in positions 24-33
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    equipment_number:str = line[23:33]
    if(equipment_number.isspace()):
        return ""
    if(re.search(r".+",equipment_number)):
        return re.search(r".+",equipment_number).group().replace(',','')
    return ""

def carrier_detail_routing(line:str) -> str:
    """
        Extracting the routing from the line that
        has the prefix G27. This is supposed to be formatted
        such that the routing is the 34 characters in positions 38-72
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    routing:str = line[37:72]
    if(routing.isspace()):
        return ""
    if(re.search(r".+",routing)):
        return re.search(r".+",routing).group().replace(',','')
    return ""

def terms_of_sale_terms_type_code(line:str) -> str:
    """
        Extracting the terms type code from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 18-19
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    terms_type_code:str = line[17:19]
    if(terms_type_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",terms_type_code)):
        return re.search(r"[A-Za-z0-9]+",terms_type_code).group().replace(',','')
    return ""

def terms_of_sale_terms_type_code_description(line:str) -> str:
    """
        Extracting the terms type code description from the line that
        has the prefix G23. This is done by collecting the code from position 18-19
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    terms_type_code:str = terms_of_sale_terms_type_code(line)
    try:
        terms_type_code_description:str = g23_terms_type_codes[terms_type_code]
        return terms_type_code_description.replace(',','')
    except:
        return ""

def terms_of_sale_terms_basis_date_code(line:str) -> str:
    """
        Extracting the terms basis date code from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 20-21
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    terms_type_code:str = line[19:21]
    if(terms_type_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",terms_type_code)):
        return re.search(r"[A-Za-z0-9]+",terms_type_code).group().replace(',','')
    return ""

def terms_of_sale_terms_basis_date_code_description(line:str) -> str:
    """
        Extracting the terms basis date code description from the line that
        has the prefix G23. This is done by collecting the code from position 20-21
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    terms_basis_date_code:str = terms_of_sale_terms_basis_date_code(line)
    try:
        terms_basis_date_code_description:str = g23_terms_basis_date_codes[terms_basis_date_code]
        return terms_basis_date_code_description.replace(',','')
    except:
        return ""

def terms_of_sale_terms_start_date(line:str) -> date:
    """
        Extracting the terms start date from the line that
        has the prefix G23. This is supposed to be formatted
        such that the date is the 6 digits in positions 22-27 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        invoice_datetime = to_datetime(line[21:27],format='%y%m%d')
        return date(invoice_datetime.year,invoice_datetime.month,invoice_datetime.day)
    except:
        return ""

def terms_of_sale_terms_due_date_qualifier_code(line:str) -> str:
    """
        Extracting the terms basis date code from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 28-29
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    terms_due_date_qualifier_code:str = line[27:29]
    if(terms_due_date_qualifier_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",terms_due_date_qualifier_code)):
        return re.search(r"[A-Za-z0-9]+",terms_due_date_qualifier_code).group().replace(',','')
    return ""

def terms_of_sale_terms_due_date_qualifier_code_description(line:str) -> str:
    """
        Extracting the terms basis date code description from the line that
        has the prefix G23. This is done by collecting the code from position 28-29
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    terms_due_date_qualifier_code:str = terms_of_sale_terms_due_date_qualifier_code(line)
    try:
        terms_due_date_qualifier_code_description:str = g23_terms_due_date_qualifier_codes[terms_due_date_qualifier_code]
        return terms_due_date_qualifier_code_description.replace(',','')
    except:
        return ""

def terms_of_sale_decimal_positions_one(line:str) -> int:
    """
        Extracting the first decimal position from the line that
        has the prefix G23. This is done by collecting the number from position 30
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[29])
    except:
        return 0

def terms_of_sale_terms_discount_percent(line:str) -> float:
    """
        Extracting the terms discount percent from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-6 characters in positions 31-36
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = terms_of_sale_decimal_positions_one(line)
    terms_discount_percent:str = line[30:36]
    if(re.search(r"[0-9]+",terms_discount_percent)):
        return round(int(re.search(r"[0-9]+",terms_discount_percent).group())/(10**num_decimal_places),4)
    return ""

def terms_of_sale_terms_discount_due_date(line:str) -> date:
    """
        Extracting the terms start date from the line that
        has the prefix G23. This is supposed to be formatted
        such that the date is the 6 digits in positions 37-42 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        terms_discount_due_date = to_datetime(line[36:42],format='%y%m%d')
        return date(terms_discount_due_date.year,terms_discount_due_date.month,terms_discount_due_date.day)
    except:
        return ""

def terms_of_sale_decimal_positions_two(line:str) -> int:
    """
        Extracting the second decimal position from the line that
        has the prefix G23. This is done by collecting the number from position 43
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[42])
    except:
        return 0

def terms_of_sale_terms_discount_days_due(line:str) -> float:
    """
        Extracting the terms discount days due from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-3 characters in positions 44-46
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = terms_of_sale_decimal_positions_two(line)
    terms_discount_days_due:str = line[43:46]
    if(re.search(r"[0-9]+",terms_discount_days_due)):
        return round(int(re.search(r"[0-9]+",terms_discount_days_due).group())/(10**num_decimal_places),4)
    return ""

def terms_of_sale_terms_net_due_date(line:str) -> date:
    """
        Extracting the terms net due date from the line that
        has the prefix G23. This is supposed to be formatted
        such that the date is the 6 digits in positions 47-52 
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        terms_net_due_date = to_datetime(line[46:52],format='%y%m%d')
        return date(terms_net_due_date.year,terms_net_due_date.month,terms_net_due_date.day)
    except:
        return ""

def terms_of_sale_decimal_positions_three(line:str) -> int:
    """
        Extracting the third decimal position from the line that
        has the prefix G23. This is done by collecting the number from position 53
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[52])
    except:
        return 0

def terms_of_sale_terms_net_days(line:str) -> float:
    """
        Extracting the terms net days from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-3 characters in positions 54-56
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = terms_of_sale_decimal_positions_three(line)
    terms_net_days:str = line[53:56]
    if(re.search(r"[0-9]+",terms_net_days)):
        return round(int(re.search(r"[0-9]+",terms_net_days).group())/(10**num_decimal_places),4)
    return ""

def terms_of_sale_decimal_positions_four(line:str) -> int:
    """
        Extracting the fourth decimal position from the line that
        has the prefix G23. This is done by collecting the number from position 57
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[56])
    except:
        return 0

def terms_of_sale_terms_discount_amount(line:str) -> float:
    """
        Extracting the terms discount amount from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-10 characters in positions 58-67
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = terms_of_sale_decimal_positions_four(line)
    terms_discount_amount:str = line[57:67]
    if(re.search(r"[0-9]+",terms_discount_amount)):
        return round(int(re.search(r"[0-9]+",terms_discount_amount).group())/(10**num_decimal_places),4)
    return ""

def terms_of_sale_decimal_positions_five(line:str) -> int:
    """
        Extracting the fifth decimal position from the line that
        has the prefix G23. This is done by collecting the number from position 68
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[67])
    except:
        return 0

def terms_of_sale_discounted_amount(line:str) -> float:
    """
        Extracting the discounted amount from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-10 characters in positions 69-78
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = terms_of_sale_decimal_positions_five(line)
    terms_discount_amount:str = line[68:78]
    if(re.search(r"[0-9]+",terms_discount_amount)):
        return round(int(re.search(r"[0-9]+",terms_discount_amount).group())/(10**num_decimal_places),4)
    return ""

def terms_of_sale_decimal_positions_six(line:str) -> int:
    """
        Extracting the sixth decimal position from the line that
        has the prefix G23. This is done by collecting the number from position 79
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[78])
    except:
        return 0

def terms_of_sale_amount_subject_to_terms_discount(line:str) -> float:
    """
        Extracting the amount subject to terms discount from the line that
        has the prefix G23. This is supposed to be formatted
        such that the number is the 1-10 characters in positions 80-89
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = terms_of_sale_decimal_positions_six(line)
    terms_discount_amount:str = line[79:89]
    if(re.search(r"[0-9]+",terms_discount_amount)):
        return round(int(re.search(r"[0-9]+",terms_discount_amount).group())/(10**num_decimal_places),4)
    return ""

def terms_of_sale_free_form_message(line:str) -> str:
    """
        Extracting the terms of sale free form message from the line that
        has the prefix N9. This is supposed to be formatted
        such that the description is placed in positions 107-166
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    free_form_description:str = line[100:200]
    if(free_form_description.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+.+",free_form_description)):
        return re.search(r"[A-Za-z0-9]{1,}.+",free_form_description).group().replace(',','')
    return ""

def fob_information_shipment_method_of_payment_code(line:str) -> str:
    """
        Extracting the shipment method of payment code from the line that
        has the prefix G25. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 18-19
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    shipment_method_of_payment_code:str = line[17:19]
    if(shipment_method_of_payment_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code)):
        return re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code).group().replace(',','')
    return ""

def fob_information_shipment_method_of_payment_code_description(line:str) -> str:
    """
        Extracting the shipment method of payment code description from the line that
        has the prefix G25. This is done by collecting the code from position 18-19
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    shipment_method_of_payment_code:str = fob_information_shipment_method_of_payment_code(line)
    try:
        shipment_method_of_payment_code_description:str = g25_shipment_method_of_payment[shipment_method_of_payment_code]
        return shipment_method_of_payment_code_description.replace(',','')
    except:
        return ""

def fob_information_fob_point_code(line:str) -> str:
    """
        Extracting the F.O.B. code from the line that
        has the prefix G25. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 20-21
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    shipment_method_of_payment_code:str = line[19:21]
    if(shipment_method_of_payment_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code)):
        return re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code).group().replace(',','')
    return ""

def fob_information_fob_point_code_description(line:str) -> str:
    """
        Extracting the F.O.B. code description from the line that
        has the prefix G25. This is done by collecting the code from position 20-21
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    fob_point_code:str = fob_information_fob_point_code(line)
    try:
        fob_point_code_description:str = g25_fob_point_codes[fob_point_code]
        return fob_point_code_description.replace(',','')
    except:
        return ""

def fob_information_fob_point(line:str) -> str:
    """
        Extracting the F.O.B. point from the line that
        has the prefix G25. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 22-51
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    fob_point:str = line[21:51]
    if(fob_point.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+.+",fob_point)):
        return re.search(r"[A-Za-z0-9]+.+",fob_point).group().replace(',','')
    return ""

def name_entity_identification_code(line:str) -> str:
    """
        Extracting the entity identification code from the line that
        has the prefix N1. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 18-19
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    shipment_method_of_payment_code:str = line[17:19]
    if(shipment_method_of_payment_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code)):
        return re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code).group().replace(',','')
    return ""

def name_entity_identification_code_description(line:str) -> str:
    """
        Extracting the entity identification code description from the line that
        has the prefix N1. This is done by collecting the code from position 18-19
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    entity_identification_code:str = name_entity_identification_code(line)
    try:
        entity_identification_code_description:str = n1_entity_identifier_codes[entity_identification_code]
        return entity_identification_code_description.replace(',','')
    except:
        return ""

def name_name(line:str) -> str:
    """
        Extracting the name from the line that
        has the prefix N1. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 20-54
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    shipment_method_of_payment_code:str = line[19:54]
    if(shipment_method_of_payment_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+.+",shipment_method_of_payment_code)):
        return re.search(r"[A-Za-z0-9]+.+",shipment_method_of_payment_code).group().replace(',','')
    return ""

def name_identification_code_qualifier_code(line:str) -> str:
    """
        Extracting the identification code qualifier code from the line that
        has the prefix N1. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 55-56
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    identification_code_qualifier_code:str = line[54:56]
    if(identification_code_qualifier_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",identification_code_qualifier_code)):
        return re.search(r"[A-Za-z0-9]+",identification_code_qualifier_code).group().replace(',','')
    return ""

def name_identification_code_qualifier_description(line:str) -> str:
    """
        Extracting the entity identification code qualifier code description from the line that
        has the prefix N1. This is done by collecting the code from position 55-56
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    entity_identification_code:str = name_identification_code_qualifier_code(line)
    try:
        entity_identification_code_description:str = n1_identifier_code_qualifier_codes[entity_identification_code]
        return entity_identification_code_description.replace(',','')
    except:
        return ""

def name_identification_code_code(line:str) -> str:
    """
        Extracting the identification code code from the line that
        has the prefix N1. This is supposed to be formatted
        such that the number is the 1-3 digits in positions 57-73
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    identification_code_code:str = line[56:73]
    if(identification_code_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+.+",identification_code_code)):
        return re.search(r"[A-Za-z0-9]+.+",identification_code_code).group()
    return ""

def address_information(line:str) -> str:
    return line[17:52].replace(',','')

def geographic_location_city_name(line:str) -> str:
    return line[17:47].replace(',','')

def geographic_location_state_province_code(line:str) -> str:
    return line[47:49].replace(',','')

def geographic_location_postal_code(line:str) -> str:
    return line[49:58].replace(',','')

def allowance_or_charge_allowance_or_charge_code(line:str) -> str:
    """
        Extracting the allowance or charge code from the line that
        has the prefix G72. This is supposed to be formatted
        such that the number is the 1-3 digits in positions 18-20
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    shipment_method_of_payment_code:str = line[17:20]
    if(shipment_method_of_payment_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code)):
        return re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code).group()
    return ""

def allowance_or_charge_allowance_or_charge_code_description(line:str) -> str:
    """
        Extracting the allowance or charge code description from the line that
        has the prefix G72. This is done by collecting the code from position 18-20
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    allowance_or_charge_code:str = allowance_or_charge_allowance_or_charge_code(line)
    try:
        allowance_or_charge_code_description:str = g72_allowance_or_charge_codes[allowance_or_charge_code]
        return allowance_or_charge_code_description.replace(',','')
    except:
        return ""

def allowance_or_charge_allowance_or_charge_method_of_handling_code(line:str) -> str:
    """
        Extracting the allowance or charge of handling code from the line that
        has the prefix G72. This is supposed to be formatted
        such that the number is the 1-3 digits in positions 21-22
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    shipment_method_of_payment_code:str = line[20:22]
    if(shipment_method_of_payment_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code)):
        return re.search(r"[A-Za-z0-9]+",shipment_method_of_payment_code).group()
    return ""

def allowance_or_charge_allowance_or_charge_method_of_handling_code_description(line:str) -> str:
    """
        Extracting the allowance or charge method of handling code description from the line that
        has the prefix G72. This is done by collecting the code from position 21-22
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    allowance_or_charge_code:str = allowance_or_charge_allowance_or_charge_method_of_handling_code(line)
    try:
        allowance_or_charge_code_description:str = g72_allowance_or_charge_method_of_handling_codes[allowance_or_charge_code]
        return allowance_or_charge_code_description.replace(',','')
    except:
        return ""

def allowance_or_charge_decimal_positions_one(line:str) -> int:
    """
        Extracting the first decimal position from the line that
        has the prefix G72. This is done by collecting the number from position 55
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[54])
    except:
        return 0

def allowance_or_charge_allowance_or_charge_rate(line:str) -> float:
    """
        Extracting the terms discount percent from the line that
        has the prefix G72. This is supposed to be formatted
        such that the number is the 3-9 characters in positions 56-64
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = allowance_or_charge_decimal_positions_one(line)
    allowance_or_charge_rate:str = line[55:64]
    if(re.search(r"[0-9]+",allowance_or_charge_rate)):
        return round(int(re.search(r"[0-9]+",allowance_or_charge_rate).group())/(10**num_decimal_places),4)
    return ""

def allowance_or_charge_decimal_positions_two(line:str) -> int:
    """
        Extracting the second decimal position from the line that
        has the prefix G72. This is done by collecting the number from position 65
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[64])
    except:
        return 0

def allowance_or_charge_allowance_or_charge_quantity(line:str) -> float:
    """
        Extracting the terms discount percent from the line that
        has the prefix G72. This is supposed to be formatted
        such that the number is the 3-10 characters in positions 66-75
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = allowance_or_charge_decimal_positions_two(line)
    allowance_or_charge_quantity:str = line[65:75]
    if(re.search(r"[0-9]+",allowance_or_charge_quantity)):
        return round(int(re.search(r"[0-9]+",allowance_or_charge_quantity).group())/(10**num_decimal_places),4)
    return ""

def allowance_or_charge_unit_or_basis_for_measurement_code(line:str) -> str:
    """
        Extracting the unit of basis for measurement code from the line that
        has the prefix G72. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 76-77
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    unit_or_basis_for_measurement_code:str = line[75:77]
    if(unit_or_basis_for_measurement_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code)):
        return re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code).group()
    return ""

def allowance_or_charge_unit_or_basis_for_measurement_code_description(line:str) -> str:
    """
        Extracting the unit or basis for measurement code description from the line that
        has the prefix G72. This is done by collecting the code from position 76-77
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    unit_or_basis_for_measurement_code:str = allowance_or_charge_unit_or_basis_for_measurement_code(line)
    try:
        unit_or_basis_for_measurement_code_description:str = g72_unit_or_basis_for_measurement_codes[unit_or_basis_for_measurement_code]
        return unit_or_basis_for_measurement_code_description.replace(',','')
    except:
        return ""

def allowance_or_charge_decimal_positions_three(line:str) -> int:
    """
        Extracting the third decimal position from the line that
        has the prefix G72. This is done by collecting the number from position 78
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[77])
    except:
        return 0

def allowance_or_charge_allowance_or_charge_total_amount(line:str) -> float:
    """
        Extracting the terms discount percent from the line that
        has the prefix G72. This is supposed to be formatted
        such that the number is the 3-9 characters in positions 79-87
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = allowance_or_charge_decimal_positions_three(line)
    allowance_or_charge_total_amount:str = line[78:87]
    if(re.search(r"[0-9]+",allowance_or_charge_total_amount)):
        return round(int(re.search(r"[0-9]+",allowance_or_charge_total_amount).group())/(10**num_decimal_places),4)
    return ""

def allowance_or_charge_free_form_description(line:str) -> str:
    """
        Extracting the allowance or charge free form description from the line that
        has the prefix G73. This is supposed to be formatted
        such that the description is placed in positions 18-62
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    free_form_description:str = line[17:62]
    if(free_form_description.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]{1,}.+",free_form_description)):
        return re.search(r"[A-Za-z0-9]{1,}.+",free_form_description).group().replace(',','')
    return ""

def item_detail_invoice_decimal_positions_one(line:str) -> int:
    """
        Extracting the first decimal position from the line that
        has the prefix G17. This is done by collecting the number from position 18
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[17])
    except:
        return 0

def item_detail_invoice_quantity_invoiced(line:str) -> float:
    """
        Extracting the quantity invoiced from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 3-10 characters in positions 19-28
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = item_detail_invoice_decimal_positions_one(line)
    quantity_invoiced:str = line[18:28]
    if(re.search(r"[0-9]+",quantity_invoiced)):
        return round(int(re.search(r"[0-9]+",quantity_invoiced).group())/(10**num_decimal_places),4)
    return ""

def item_detail_invoice_unit_or_basis_for_measurement_code_one(line:str) -> str:
    """
        Extracting the first unit of basis for measurement code from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 29-30
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    unit_or_basis_for_measurement_code:str = line[28:30]
    if(unit_or_basis_for_measurement_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code)):
        return re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code).group()
    return ""

def item_detail_invoice_unit_or_basis_for_measurement_code_description_one(line:str) -> str:
    """
        Extracting the first unit or basis for measurement code description from the line that
        has the prefix G17. This is done by collecting the code from position 29-30
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    unit_or_basis_for_measurement_code:str = item_detail_invoice_unit_or_basis_for_measurement_code_one(line)
    try:
        unit_or_basis_for_measurement_code_description:str = g17_unit_or_basis_for_measurement_codes[unit_or_basis_for_measurement_code]
        return unit_or_basis_for_measurement_code_description.replace(',','')
    except:
        return ""

def item_detail_invoice_decimal_positions_two(line:str) -> int:
    """
        Extracting the second decimal position from the line that
        has the prefix G17. This is done by collecting the number from position 31
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[30])
    except:
        return 0

def item_detail_invoice_item_list_cost(line:str) -> float:
    """
        Extracting the list cost from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 3-9 characters in positions 32-40
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = item_detail_invoice_decimal_positions_two(line)
    list_cost:str = line[31:40]
    if(re.search(r"[0-9]+",list_cost)):
        return round(int(re.search(r"[0-9]+",list_cost).group())/(10**num_decimal_places),4)
    return ""

def item_detail_invoice_upc_case_code(line:str) -> str:
    """
        Extracting the upc case code from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 3-12 digits in positions 41-52
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    upc_case_code:str = line[40:52]
    if(upc_case_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",upc_case_code)):
        return re.search(r"[A-Za-z0-9]+",upc_case_code).group().replace(',','')
    return ""

def item_detail_invoice_product_service_id_qualifier_code_one(line:str) -> str:
    """
        Extracting the first product/service ID qualifier code from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 53-54
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    product_service_id_qualifier_code:str = line[52:54]
    if(product_service_id_qualifier_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",product_service_id_qualifier_code)):
        return re.search(r"[A-Za-z0-9]+",product_service_id_qualifier_code).group()
    return ""

def item_detail_invoice_product_service_id_qualifier_code_description_one(line:str) -> str:
    """
        Extracting the first product/service ID qualifier code description from the line that
        has the prefix G17. This is done by collecting the code from position 53-54
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    product_service_id_qualifier_code:str = item_detail_invoice_product_service_id_qualifier_code_one(line)
    try:
        product_service_id_qualifier_code_description:str = g17_product_service_id_qualifier_codes[product_service_id_qualifier_code]
        return product_service_id_qualifier_code_description.replace(',','')
    except:
        return ""

def item_detail_invoice_product_service_id_one(line:str) -> str:
    """
        Extracting the first product/service ID from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 55-84
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    product_service_id:str = line[54:84]
    if(product_service_id.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+.+",product_service_id)):
        return re.search(r"[A-Za-z0-9]+.+",product_service_id).group().replace(',','')
    return ""

def item_detail_invoice_product_service_id_qualifier_code_two(line:str) -> str:
    """
        Extracting the second product/service ID qualifier code from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 85-86
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    product_service_id_qualifier_code:str = line[84:86]
    if(product_service_id_qualifier_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",product_service_id_qualifier_code)):
        return re.search(r"[A-Za-z0-9]+",product_service_id_qualifier_code).group().replace(',','')
    return ""

def item_detail_invoice_product_service_id_qualifier_code_description_two(line:str) -> str:
    """
        Extracting the second product/service ID qualifier code description from the line that
        has the prefix G17. This is done by collecting the code from position 85-86
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    product_service_id_qualifier_code:str = item_detail_invoice_product_service_id_qualifier_code_two(line)
    try:
        product_service_id_qualifier_code_description:str = g17_product_service_id_qualifier_codes[product_service_id_qualifier_code]
        return product_service_id_qualifier_code_description.replace(',','')
    except:
        return ""

def item_detail_invoice_product_service_id_two(line:str) -> str:
    """
        Extracting the second product/service ID from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 87-116
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    product_service_id:str = line[86:116]
    if(product_service_id.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+.+",product_service_id)):
        return re.search(r"[A-Za-z0-9]+.+",product_service_id).group().replace(',','')
    return ""

def item_detail_invoice_decimal_positions_three(line:str) -> int:
    """
        Extracting the third decimal position from the line that
        has the prefix G17. This is done by collecting the number from position 120
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[119])
    except:
        return 0

def item_detail_invoice_number_of_units_shipped(line:str) -> float:
    """
        Extracting the number of units shipped from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 3-9 characters in positions 121-130
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = item_detail_invoice_decimal_positions_three(line)
    number_of_units_shipped:str = line[120:130]
    if(re.search(r"[0-9]+",number_of_units_shipped)):
        return round(int(re.search(r"[0-9]+",number_of_units_shipped).group())/(10**num_decimal_places),4)
    return ""

def item_detail_invoice_unit_or_basis_for_measurement_code_two(line:str) -> str:
    """
        Extracting the second unit of basis for measurement code from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 131-132
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    unit_or_basis_for_measurement_code:str = line[130:132]
    if(unit_or_basis_for_measurement_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code)):
        return re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code).group().replace(',','')
    return ""

def item_detail_invoice_unit_or_basis_for_measurement_code_description_two(line:str) -> str:
    """
        Extracting the second unit or basis for measurement code description from the line that
        has the prefix G17. This is done by collecting the code from position 29-30
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    unit_or_basis_for_measurement_code:str = item_detail_invoice_unit_or_basis_for_measurement_code_two(line)
    try:
        unit_or_basis_for_measurement_code_description:str = g17_unit_or_basis_for_measurement_codes[unit_or_basis_for_measurement_code]
        return unit_or_basis_for_measurement_code_description.replace(',','')
    except:
        return ""

def item_detail_invoice_decimal_positions_four(line:str) -> int:
    """
        Extracting the fourth decimal position from the line that
        has the prefix G17. This is done by collecting the number from position 165
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[164])
    except:
        return 0

def item_detail_invoice_monetary_amount(line:str) -> float:
    """
        Extracting the list cost from the line that
        has the prefix G17. This is supposed to be formatted
        such that the number is the 3-15 characters in positions 166-180
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = item_detail_invoice_decimal_positions_four(line)
    monetary_amount:str = line[165:180]
    if(re.search(r"[0-9]+",monetary_amount)):
        return round(int(re.search(r"[0-9]+",monetary_amount).group())/(10**num_decimal_places),4)
    return ""

def line_item_detail_description_free_form_description(line:str) -> str:
    """
        Extracting the allowance or charge free form description from the line that
        has the prefix G73. This is supposed to be formatted
        such that the description is placed in positions 18-62
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    free_form_description:str = line[17:62]
    if(free_form_description.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]{1,}.+",free_form_description)):
        return re.search(r"[A-Za-z0-9]{1,}.+",free_form_description).group().replace(',','')
    return ""

def item_packing_detail_decimal_positions_one(line:str) -> int:
    """
        Extracting the first decimal position from the line that
        has the prefix G20. This is done by collecting the number from position 18
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[17])
    except:
        return 0

def item_packing_detail_pack_or_number_inner_pack_units_per_outer_pack_unit(line:str) -> float:
    """
        Extracting the pack or number of inner pack units per outer pack unit from the line that
        has the prefix G20. This is supposed to be formatted
        such that the number is the 3-10 characters in positions 19-24
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = item_packing_detail_decimal_positions_one(line)
    pack_or_number_inner_pack_units_per_outer_pack_unit:str = line[18:24]
    if(re.search(r"[0-9]+",pack_or_number_inner_pack_units_per_outer_pack_unit)):
        return round(int(re.search(r"[0-9]+",pack_or_number_inner_pack_units_per_outer_pack_unit).group())/(10**num_decimal_places),4)
    return ""

def total_invoice_quantity_decimal_positions_one(line:str) -> int:
    """
        Extracting the first decimal position from the line that
        has the prefix G31. This is done by collecting the number from position 18
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[17])
    except:
        return 0

def total_invoice_quantity_number_of_units_shipped(line:str) -> float:
    """
        Extracting the number of units shipped from the line that
        has the prefix G31. This is supposed to be formatted
        such that the number is the 3-10 characters in positions 19-28
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = total_invoice_quantity_decimal_positions_one(line)
    number_of_units_shipped:str = line[18:28]
    if(re.search(r"[0-9]+",number_of_units_shipped)):
        return round(int(re.search(r"[0-9]+",number_of_units_shipped).group())/(10**num_decimal_places),4)
    return ""

def total_invoice_quantity_unit_or_basis_for_measurement_code_one(line:str) -> str:
    """
        Extracting the first unit of basis for measurement code from the line that
        has the prefix G31. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 29-30
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    unit_or_basis_for_measurement_code:str = line[28:30]
    if(unit_or_basis_for_measurement_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code)):
        return re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code).group()
    return ""

def total_invoice_quantity_unit_or_basis_for_measurement_code_description_one(line:str) -> str:
    """
        Extracting the first unit or basis for measurement code description from the line that
        has the prefix G31. This is done by collecting the code from position 29-30
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    unit_or_basis_for_measurement_code:str = item_detail_invoice_unit_or_basis_for_measurement_code_one(line)
    try:
        unit_or_basis_for_measurement_code_description:str = g17_unit_or_basis_for_measurement_codes[unit_or_basis_for_measurement_code]
        return unit_or_basis_for_measurement_code_description.replace(',','')
    except:
        return ""

def total_invoice_quantity_decimal_positions_two(line:str) -> int:
    """
        Extracting the second decimal position from the line that
        has the prefix G31. This is done by collecting the number from position 31
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[30])
    except:
        return 0

def total_invoice_quantity_weight(line:str) -> float:
    """
        Extracting the weight from the line that
        has the prefix G31. This is supposed to be formatted
        such that the number is the 3-10 characters in positions 32-41
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = total_invoice_quantity_decimal_positions_two(line)
    weight:str = line[31:41]
    if(re.search(r"[0-9]+",weight)):
        return round(int(re.search(r"[0-9]+",weight).group())/(10**num_decimal_places),4)
    return ""

def total_invoice_quantity_unit_or_basis_for_measurement_code_two(line:str) -> str:
    """
        Extracting the second unit of basis for measurement code from the line that
        has the prefix G31. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 42-43
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    unit_or_basis_for_measurement_code:str = line[41:43]
    if(unit_or_basis_for_measurement_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code)):
        return re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code).group().replace(',','')
    return ""

def total_invoice_quantity_unit_or_basis_for_measurement_code_description_two(line:str) -> str:
    """
        Extracting the second unit or basis for measurement code description from the line that
        has the prefix G31. This is done by collecting the code from position 42-43
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    unit_or_basis_for_measurement_code:str = total_invoice_quantity_unit_or_basis_for_measurement_code_two(line)
    try:
        unit_or_basis_for_measurement_code_description:str = g17_unit_or_basis_for_measurement_codes[unit_or_basis_for_measurement_code]
        return unit_or_basis_for_measurement_code_description.replace(',','')
    except:
        return ""

def total_invoice_quantity_decimal_positions_three(line:str) -> int:
    """
        Extracting the third decimal position from the line that
        has the prefix G31. This is done by collecting the number from position 44
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[43])
    except:
        return 0

def total_invoice_quantity_volume(line:str) -> float:
    """
        Extracting the volume from the line that
        has the prefix G31. This is supposed to be formatted
        such that the number is the 3-8 characters in positions 45-52
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = total_invoice_quantity_decimal_positions_three(line)
    volume:str = line[44:52]
    if(re.search(r"[0-9]+",volume)):
        return round(int(re.search(r"[0-9]+",volume).group())/(10**num_decimal_places),4)
    return ""

def total_invoice_quantity_unit_or_basis_for_measurement_code_three(line:str) -> str:
    """
        Extracting the third unit of basis for measurement code from the line that
        has the prefix G31. This is supposed to be formatted
        such that the number is the 1-2 digits in positions 53-54
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    unit_or_basis_for_measurement_code:str = line[52:54]
    if(unit_or_basis_for_measurement_code.isspace()):
        return ""
    if(re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code)):
        return re.search(r"[A-Za-z0-9]+",unit_or_basis_for_measurement_code).group().replace(',','')
    return ""

def total_invoice_quantity_unit_or_basis_for_measurement_code_description_three(line:str) -> str:
    """
        Extracting the third unit or basis for measurement code description from the line that
        has the prefix G31. This is done by collecting the code from position 53-54
        according to Scan_0001_08-09-2023_1136.pdf and then returning the definition
        of the code.
    """
    unit_or_basis_for_measurement_code:str = total_invoice_quantity_unit_or_basis_for_measurement_code_three(line)
    try:
        unit_or_basis_for_measurement_code_description:str = g17_unit_or_basis_for_measurement_codes[unit_or_basis_for_measurement_code]
        return unit_or_basis_for_measurement_code_description.replace(',','')
    except:
        return ""

def total_dollars_summary_decimal_positions_one(line:str) -> int:
    """
        Extracting the first decimal position from the line that
        has the prefix G33. This is done by collecting the number from position 18
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    try:
        return int(line[17])
    except:
        return 0

def total_dollars_summary_total_invoice_amount(line:str) -> float:
    """
        Extracting the total invoice amount from the line that
        has the prefix G33. This is supposed to be formatted
        such that the number is the 3-10 characters in positions 19-28
        according to Scan_0001_08-09-2023_1136.pdf.
    """
    num_decimal_places:int = total_dollars_summary_decimal_positions_one(line)
    total_invoice_amount:str = line[18:28]
    if(re.search(r"[0-9]+",total_invoice_amount)):
        return round(int(re.search(r"[0-9]+",total_invoice_amount).group())/(10**num_decimal_places),4)
    return ""

def get_table_names(cursor_1:Cursor) -> list[str]:
    sql_query:str="""
                    SELECT TABLE_NAME
                        FROM barney.source_MitchellGrocery_2024.INFORMATION_SCHEMA.TABLES
                        WHERE 
                            TABLE_TYPE = 'BASE TABLE' 
                            AND TABLE_NAME LIKE 'ATG_SYS_EDI880%'
                            AND TABLE_SCHEMA='dbo'
                            AND (TABLE_CATALOG='source_MitchellGrocery_2023' OR TABLE_CATALOG='source_MitchellGrocery_2024')

                    UNION ALL

                    SELECT TABLE_NAME
                        FROM apu.source_MitchellGrocery_2023.INFORMATION_SCHEMA.TABLES
                        WHERE 
                            TABLE_TYPE = 'BASE TABLE' 
                            AND TABLE_NAME LIKE 'ATG_SYS_EDI880%'
                            AND TABLE_SCHEMA='dbo'
                            AND (TABLE_CATALOG='source_MitchellGrocery_2023' OR TABLE_CATALOG='source_MitchellGrocery_2024')
                        ORDER BY TABLE_NAME
                """
    cursor_1.execute(sql_query)
    names = cursor_1.fetchall()
    table_names:list[str] = []
    for name in names:
        table_names.append(str(name[0]))
    return table_names

def extract_g01(line:str) -> list[str]:
    row:list[str] = []
    row.append(invoice_identification_invoice_date(line))
    row.append(invoice_identification_invoice_number(line))
    row.append(invoice_identification_purchase_order_date(line))
    row.append(invoice_identification_purchase_order_number(line))
    return row

def extract_n9(line:str) -> list[str]:
    row:list[str] = []
    row.append(reference_number_reference_number_qualifier_code(line))
    row.append(reference_number_reference_number_qualifier_code_description(line))
    row.append(reference_number_reference_number(line))
    row.append(reference_number_free_form_description(line))
    return row

def extract_g62(line:str) -> list[str]:
    row:list[str] = []
    row.append(date_time_date_qualifier(line))
    row.append(date_time_date_qualifier_description(line))
    row.append(date_time_date_yymmdd(line))
    return row

def extract_nte(line:str) -> list[str]:
    row:list[str] = []
    row.append(note_special_instruction_reference_code(line))
    row.append(note_special_instruction_reference_code_description(line))
    row.append(note_special_instruction_free_form_description(line))
    return row

def extract_g27(line:str) -> list[str]:
    row:list[str] = []
    row.append(carrier_detail_transportation_method_type_code(line))
    row.append(carrier_detail_transportation_method_type_code_description(line))
    row.append(carrier_detail_equipment_number(line))
    row.append(carrier_detail_routing(line))
    return row

def extract_g23(line:str) -> list[str]:
    row:list[str] = []
    row.append(terms_of_sale_terms_type_code(line))
    row.append(terms_of_sale_terms_type_code_description(line))
    row.append(terms_of_sale_terms_basis_date_code(line))
    row.append(terms_of_sale_terms_basis_date_code_description(line))
    row.append(terms_of_sale_terms_start_date(line))
    row.append(terms_of_sale_terms_due_date_qualifier_code(line))
    row.append(terms_of_sale_terms_due_date_qualifier_code_description(line))
    row.append(terms_of_sale_terms_discount_percent(line))
    row.append(terms_of_sale_terms_discount_due_date(line))
    row.append(terms_of_sale_terms_discount_days_due(line))
    row.append(terms_of_sale_terms_net_due_date(line))
    row.append(terms_of_sale_terms_net_days(line))
    row.append(terms_of_sale_terms_discount_amount(line))
    row.append(terms_of_sale_discounted_amount(line))
    row.append(terms_of_sale_amount_subject_to_terms_discount(line))
    row.append(terms_of_sale_free_form_message(line))
    return row

def extract_g25(line:str) -> list[str]:
    row:list[str] = []
    row.append(fob_information_shipment_method_of_payment_code(line))
    row.append(fob_information_shipment_method_of_payment_code_description(line))
    row.append(fob_information_fob_point_code(line))
    row.append(fob_information_fob_point_code_description(line))
    row.append(fob_information_fob_point(line))
    return row

def extract_n1(line:str) -> list[str]:
    row:list[str] = []
    row.append(name_entity_identification_code(line))
    row.append(name_entity_identification_code_description(line))
    row.append(name_name(line))
    row.append(name_identification_code_qualifier_code(line))
    row.append(name_identification_code_qualifier_description(line))
    row.append(name_identification_code_code(line))
    return row

def extract_n3(line:str) -> list[str]:
    row:list[str] = []
    row.append(address_information(line))
    return row

def extract_n4(line:str) -> list[str]:
    row:list[str] = []
    row.append(geographic_location_city_name(line))
    row.append(geographic_location_state_province_code(line))
    row.append(geographic_location_postal_code(line))
    return row

def extract_g31(line:str) -> list[str]:
    row:list[str] = []
    row.append(total_invoice_quantity_number_of_units_shipped(line))
    row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_one(line))
    row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_one(line))
    row.append(total_invoice_quantity_weight(line))
    row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_two(line))
    row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_two(line))
    row.append(total_invoice_quantity_volume(line))
    row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_three(line))
    row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_three(line))
    return row

def extract_g33(line:str) -> list[str]:
    row:list[str] = []
    row.append(total_dollars_summary_total_invoice_amount(line))
    return row

def extract_header_to_dataframe(invoices:list[list[str]], columns:list[str]) -> DataFrame:
    rows:list[list[str]] = []
    current_row:list[str] = []
    for invoice in invoices:
        system('cls')
        print(f"Invoices Read: {invoices.index(invoice)}\nInvoices Left: {len(invoices)-invoices.index(invoice)}")
        current_row.clear()
        for line in invoice:
            if("G01" in line[:10]):
                current_row.append(invoice_identification_invoice_date(line))
                current_row.append(invoice_identification_invoice_number(line))
                current_row.append(invoice_identification_purchase_order_date(line))
                current_row.append(invoice_identification_purchase_order_number(line))
                break
        for line in invoice:
            if("G31" in line[:10]):
                current_row.append(total_invoice_quantity_number_of_units_shipped(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_one(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_one(line))
                current_row.append(total_invoice_quantity_weight(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_two(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_two(line))
                current_row.append(total_invoice_quantity_volume(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_three(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_three(line))
                break
        for line in invoice:
            if("G33" in line[:10]):
                current_row.append(total_dollars_summary_total_invoice_amount(line))
                break
        rows.append(current_row)
        current_row:list[str] = []
    return DataFrame(data=rows,columns=columns)

def extract_header_to_list(invoices:list[list[str]]) -> list[list[str]]:
    rows:list[list[str]] = []
    current_row:list[str] = []
    for invoice in invoices:
        if(invoices.index(invoice)%5000==0):
            system('cls')
            print(f"Invoices Read: {invoices.index(invoice)}\nInvoices Left: {len(invoices)-invoices.index(invoice)}")
        current_row.clear()
        for line in invoice:
            if("G01" in line[:10]):
                current_row.append(invoice_identification_invoice_date(line))
                current_row.append(invoice_identification_invoice_number(line))
                current_row.append(invoice_identification_purchase_order_date(line))
                current_row.append(invoice_identification_purchase_order_number(line))
                break
        for line in invoice:
            if("G31" in line[:10]):
                current_row.append(total_invoice_quantity_number_of_units_shipped(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_one(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_one(line))
                current_row.append(total_invoice_quantity_weight(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_two(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_two(line))
                current_row.append(total_invoice_quantity_volume(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_three(line))
                current_row.append(total_invoice_quantity_unit_or_basis_for_measurement_code_description_three(line))
                break
        for line in invoice:
            if("G33" in line[:10]):
                current_row.append(total_dollars_summary_total_invoice_amount(line))
                break
        rows.append(current_row)
        current_row:list[str] = []
    return rows

def extract_g72(line:str) -> list[str]:
    row:list[str] = []
    row.append(allowance_or_charge_allowance_or_charge_code(line))
    row.append(allowance_or_charge_allowance_or_charge_code_description(line))
    row.append(allowance_or_charge_allowance_or_charge_method_of_handling_code(line))
    row.append(allowance_or_charge_allowance_or_charge_method_of_handling_code_description(line))
    row.append(allowance_or_charge_allowance_or_charge_rate(line))
    row.append(allowance_or_charge_allowance_or_charge_quantity(line))
    row.append(allowance_or_charge_unit_or_basis_for_measurement_code(line))
    row.append(allowance_or_charge_unit_or_basis_for_measurement_code_description(line))
    row.append(allowance_or_charge_allowance_or_charge_total_amount(line))
    return row

def extract_g73(line:str) -> list[str]:
    row:list[str] = []
    row.append(allowance_or_charge_free_form_description(line))
    return row

def extract_g17(line:str) -> list[str]:
    row:list[str] = []
    row.append(item_detail_invoice_quantity_invoiced(line))
    row.append(item_detail_invoice_unit_or_basis_for_measurement_code_one(line))
    row.append(item_detail_invoice_unit_or_basis_for_measurement_code_description_one(line))
    row.append(item_detail_invoice_item_list_cost(line))
    row.append(item_detail_invoice_upc_case_code(line))
    row.append(item_detail_invoice_product_service_id_qualifier_code_one(line))
    row.append(item_detail_invoice_product_service_id_qualifier_code_description_one(line))
    row.append(item_detail_invoice_product_service_id_one(line))
    row.append(item_detail_invoice_product_service_id_qualifier_code_two(line))
    row.append(item_detail_invoice_product_service_id_qualifier_code_description_two(line))
    row.append(item_detail_invoice_product_service_id_two(line))
    row.append(item_detail_invoice_number_of_units_shipped(line))
    row.append(item_detail_invoice_unit_or_basis_for_measurement_code_two(line))
    row.append(item_detail_invoice_unit_or_basis_for_measurement_code_description_two(line))
    row.append(item_detail_invoice_monetary_amount(line))
    return row

def extract_g69(line:str) -> list[str]:
    row:list[str] = []
    row.append(line_item_detail_description_free_form_description(line))
    return row

def extract_g20(line:str) -> list[str]:
    row:list[str] = []
    row.append(item_packing_detail_pack_or_number_inner_pack_units_per_outer_pack_unit(line))
    return row

header_table_columns:list[str] = [
                                    "ATG_Ref",
                                    
                                    "G01_Invoice_Date","G01_Invoice_Number","G01_PO_Date","G01_PO_Number",

                                    "N9_Reference_Number_Qualifier_Code","N9_Reference_Number_Qualifier_Code_Description",
                                    "N9_Reference_Number","N9_Reference_Number_Free_Form_Description",
                                    
                                    "G62_Date_Qualifier_Code","G62_Date_Qualifier_Code_Description","G62_Date",

                                    "NTE_Note_Reference_Code","NTE_Note_Reference_Code_Description","NTE_Free_Form_Message",

                                    "G27_Transportation_Method_Type_Code","G27_Transportation_Method_Type_Code_Description","G27_Equipment_Number","G27_Routing",

                                    "G23_Terms_Type_Code","G23_Terms_Type_Code_Description","G23_Terms_Basis_Date_Code","G23_Terms_Basis_Date_Code_Description",
                                    "G23_Terms_Start_Date","G23_Terms_Due_Date_Qualifier_Code","G23_Terms_Due_Date_Qualifier_Code_Description",
                                    "G23_Terms_Discount_Percent","G23_Terms_Discount_Due_Date","G23_Terms_Discount_Days_Due","G23_Terms_Net_Due_Date","G23_Terms_Net_Days",
                                    "G23_Terms_Discount_Amount","G23_Discounted_Amount","G23_Amount_Subject_to_Terms_Discount","G23_Free_Form_Message",

                                    "G25_Shipment_Method_of_Payment_Code","G25_Shipment_Method_of_Payment_Code_Description",
                                    "G25_FOB_Point_Code","G25_FOB_Point_Code_Description","G25_FOB_Point",
                                    
                                    "N1_Entity_Identification_Code","N1_Entity_Identification_Code_Description",
                                    "N1_Name","N1_ID_Code_Qualifier","N1_ID_Code_Qualifier_Description","N1_ID_Code",
                                    "N3_Address_Info","N4_City_Name","N4_State_Province_Code","N4_Postal_Code",

                                    "G31_Num_Units_Shipped","G31_Unit_or_Basis_For_Measurement_Code_1","G31_Unit_or_Basis_For_Measurement_Code_Description_1",
                                    "G31_Weight","G31_Unit_or_Basis_For_Measurement_Code_2","G31_Unit_or_Basis_For_Measurement_Code_Description_2",
                                    "G31_Volume","G31_Unit_or_Basis_For_Measurement_Code_3","G31_Unit_or_Basis_For_Measurement_Code_Description_3",
                                    
                                    "G33_Total_Invoice_Amount"
                                  ]
header_allowance_table_columns:list[str] = [
        "ATG_Ref","ATG_HDR_Ref",
        "G72_Allowance_Or_Charge_Code","G72_Allowance_Or_Charge_Code_Description",
        "G72_Allowance_Or_Charge_Method_Of_Handling_Code","G72_Allowance_Or_Charge_Method_Of_Handling_Code_Description",
        "G72_Allowance_Or_Charge_Rate","G72_Allowance_Or_Charge_Quantity","G72_Unit_Basis_For_Measurement_Code","G72_Unit_Basis_For_Measurement_Code_Description",
        "G72_Allowance_Or_Charge_Total_Amount","G73_Free_Form_Description"
    ]
detail_table_columns:list[str] = [
        "ATG_Ref","ATG_HDR_Ref",
        "G17_Quantity_Invoiced","G17_Unit_Basis_For_Measurement_Code_1","G17_Unit_Basis_For_Measurement_Code_Description_1",
        "G17_Item_List_Cost","G17_UPC_Case_Code",
        "G17_Product_Service_ID_Qualifier_Code_1","G17_Product_Service_ID_Qualifier_Code_Description_1","G17_Product_Service_ID_1",
        "G17_Product_Service_ID_Qualifier_Code_2","G17_Product_Service_ID_Qualifier_Code_Description_2","G17_Product_Service_ID_2",
        "G17_Number_Of_Units_Shipped","G17_Unit_Basis_For_Measurement_Code_2","G17_Unit_Basis_For_Measurement_Code_Description_2",
        "G17_Monetary_Amount","G69_Free_Form_Description",
        "G20_Pack_Number_Of_Inner_Pack_Units_Per_Outer_Pack_Unit"
    ]
detail_allowance_table_columns:list[str] = [
        "ATG_Ref","ATG_HDR_Ref","ATG_DTL_Ref",
        "G72_Allowance_Or_Charge_Code","G72_Allowance_Or_Charge_Code_Description",
        "G72_Allowance_Or_Charge_Method_Of_Handling_Code","G72_Allowance_Or_Charge_Method_Of_Handling_Code_Description",
        "G72_Allowance_Or_Charge_Rate","G72_Allowance_Or_Charge_Quantity","G72_Unit_Basis_For_Measurement_Code","G72_Unit_Basis_For_Measurement_Code_Description",
        "G72_Allowance_Or_Charge_Total_Amount","G73_Free_Form_Description"
    ]

table_index:int = 0
atg_hdr_ref:int = 1
atg_hdr_allowance_ref:int = 1
atg_dtl_ref:int = 1
atg_dtl_allowance_ref:int = 1

open(f"ATG_SYS_EDI880_ALL_Invoices.txt",'w').close()
table_names:list[str] = get_table_names(cursor_1)
for table in table_names:
    system('cls')
    print(f"SQL Tables Read: {table_names.index(table)}\nSQL Tables Left: {len(table_names)-table_names.index(table)}")
    if('2023' in table):
        cursor_1.execute(f"SELECT Edi880a\n"
                        f"FROM [apu].[source_MitchellGrocery_2023].[dbo].{table}")
        data = cursor_1.fetchall()
        with open(f"ATG_SYS_EDI880_ALL_Invoices.txt",'a') as file:
            for row in data:
                file.write(f"{str(row[0])}\n")
    else:
        cursor_1.execute(f"SELECT Edi880a\n"
                        f"FROM [barney].[source_MitchellGrocery_2024].[dbo].{table}")
        data = cursor_1.fetchall()
        with open(f"ATG_SYS_EDI880_ALL_Invoices.txt",'a') as file:
            for row in data:
                file.write(f"{str(row[0])}\n")

with open(f"ATG_SYS_EDI880_ALL_Invoices.txt",'r') as file:
    lines:list[str] = file.readlines()

invoices:list[list[str]] = []
current_invoice:list[str] = []
for line in lines:
    if("#EOT" in line):
        current_invoice.append(line)
        invoices.append(current_invoice)
        current_invoice:list[str] = []
    else:
        current_invoice.append(line)

hdr_rows:list[list[str]] = []
dtl_rows:list[list[str]] = []
dtl_allowance_rows:list[list[str]] = []
current_hdr_row:list[str] = ['']*len(header_table_columns)
current_dtl_row:list[str] = []
current_dtl_allowance_row:list[str] = []

atg_ref:int = 1
for invoice in invoices:
    if(invoices.index(invoice)%1000==0):
        system('cls')
        print(f"Invoices Read for Header Table: {invoices.index(invoice):,}\nInvoices Left for Header Table: {len(invoices)-invoices.index(invoice):,}")
    current_hdr_row:list[str] = ['']*len(header_table_columns)
    current_hdr_allowance_row:list[str] = []
    current_dtl_row:list[str] = []
    current_dtl_allowance_row:list[str] = []
    details_reached:bool = False
    current_hdr_row[0] = str(atg_ref)
    for line in invoice:
        if("G01" in line[:10]):
            current_hdr_row[1:5] = extract_g01(line)
        elif("N9" in line[:10]):
            current_hdr_row[5:9] = extract_n9(line)
        elif("G62" in line[:10]):
            current_hdr_row[9:12] = extract_g62(line)
        elif("NTE" in line[:10]):
            current_hdr_row[12:15] = extract_nte(line)
        elif("G27" in line[:10]):
            current_hdr_row[15:19] = extract_g27(line)
        elif("G23" in line[:10]):
            current_hdr_row[19:35] = extract_g23(line)
        elif("G25" in line[:10]):
            current_hdr_row[35:40] = extract_g25(line)
        elif("N1" in line[:10]):
            current_hdr_row[40:46] = extract_n1(line)
        elif("N3" in line[:10]):
            current_hdr_row[46:47] = extract_n3(line)
        elif("N4" in line[:10]):
            current_hdr_row[47:50] = extract_n4(line)
        elif("G31" in line[:10]):
            current_hdr_row[50:59] = extract_g31(line)
        elif("G33" in line[:10]):
            current_hdr_row[59] = extract_g33(line)[0]
    hdr_rows.append(current_hdr_row)
    atg_ref += 1

hdr_table:DataFrame = DataFrame(hdr_rows,columns=header_table_columns)
hdr_table.to_csv('Mitchell_EDI_880_HDR.csv',index=False)
hdr_allowance_rows:list[list[str]] = []
current_hdr_allowance_row:list[str] = ['']*len(header_allowance_table_columns)

hdr_table:DataFrame = read_csv("Mitchell_EDI_880_HDR.csv")
count:int = 0
atg_ref:int = 1
for invoice in invoices:
    if(count%1000==0):
        system('cls')
        print(f"Invoices Read for Header Allowance Table: {count:,}\nInvoices Left for Header Allowance Table: {len(invoices)-count:,}")
    count += 1
    if(True):
        current_hdr_allowance_row:list[str] = ['']*len(header_allowance_table_columns)
        atg_ref_header:int = -1
        hdr_allowance_found:bool = False
    for line in invoice:
        if("G01" in line[:10]):
            invoice_date:date = str(invoice_identification_invoice_date(line))
            invoice_number:str = str(invoice_identification_invoice_number(line))
            try:
                atg_ref_header:int = hdr_table[(hdr_table['G01_Invoice_Date']==invoice_date)&(hdr_table['G01_Invoice_Number']==invoice_number)]['ATG_Ref'].values[0]
            except:pass
            break
    if(not(atg_hdr_ref==-1)):
        current_hdr_allowance_row[0] = atg_ref
        current_hdr_allowance_row[1] = atg_ref_header
        for line in invoice:
            if("G17" in line[:10]):
                break
            if(("G72" in line[:10])and(not(hdr_allowance_found))):
                current_hdr_allowance_row[2:11] = extract_g72(line)
                hdr_allowance_found:bool = True
                if(("G73" in invoice[invoice.index(line)+1][:10])):
                    current_hdr_allowance_row[11] = extract_g73(invoice[invoice.index(line)+1])[0]
                hdr_allowance_rows.append(current_hdr_allowance_row)
                atg_ref += 1
                break

hdr_allowance_table:DataFrame = DataFrame(hdr_allowance_rows,columns=header_allowance_table_columns)
hdr_allowance_table.to_csv('Mitchell_EDI_880_HDR_Allowances.csv',index=False)

dtl_rows:list[list[str]] = []
dtl_allowance_rows:list[list[str]] = []
current_dtl_row:list[str] = ['']*len(detail_table_columns)

count:int = 0
atg_dtl_ref:int = 1
atg_dtl_allowance_ref:int = 1
header_allowance_found:bool = False
for invoice in invoices:
    if(count%100==0):
        system('cls')
        print(f"Invoices Read for Detail/Detail Allowance Table: {count:,}\nInvoices Left for Detail/Detail Allowance Table: {len(invoices)-count:,}")
    count += 1
    if(True):
        current_dtl_row:list[str] = ['']*len(detail_table_columns)
        atg_ref_header:int = -1
        header_allowance_found:bool = False
        current_dtl_allowance_row:list[str] = ['']*len(detail_allowance_table_columns)
        
    for line in invoice:
        if("G01" in line[:10]):
            invoice_date:date = str(invoice_identification_invoice_date(line))
            invoice_number:str = str(invoice_identification_invoice_number(line))
            try:
                atg_ref_header:int = hdr_table[(hdr_table['G01_Invoice_Date']==invoice_date)&(hdr_table['G01_Invoice_Number']==invoice_number)]['ATG_Ref'].values[0]
            except:pass
            break
    
    if(not(atg_ref_header==-1)):
        current_dtl_row[0] = atg_dtl_ref
        current_dtl_row[1] = atg_ref_header
        for line in range(len(invoice)):
            if("G17" in invoice[line][:10]):
                current_dtl_row[2:17] = extract_g17(invoice[line])
                hdr_allowance_found:bool = True
                index:int = line+1
                while(not("G17" in invoice[index][:10])):
                    if("G69" in invoice[index][:10]):
                        current_dtl_row[17] = extract_g69(invoice[index])[0]
                    elif("G20" in invoice[index][:10]):
                        current_dtl_row[18] = extract_g20(invoice[index])[0]
                    elif("G72" in invoice[index][:10]):
                        current_dtl_allowance_row:list[str] = ['']*len(detail_allowance_table_columns)
                        current_dtl_allowance_row[0] = atg_dtl_allowance_ref
                        current_dtl_allowance_row[1] = atg_ref_header
                        current_dtl_allowance_row[2] = atg_dtl_ref
                        current_dtl_allowance_row[3:12] = extract_g72(invoice[index])
                        if("G73" in invoice[index+1][:10]):
                            current_dtl_allowance_row[12] = extract_g73(invoice[index+1])
                        dtl_allowance_rows.append(current_dtl_allowance_row)
                        atg_dtl_allowance_ref += 1
                        current_dtl_allowance_row:list[str] = ['']*len(detail_allowance_table_columns)
                    if("G33" in invoice[index]):
                        break
                    index += 1
                dtl_rows.append(current_dtl_row)
                atg_dtl_ref += 1
                current_dtl_row:list[str] = ['']*len(detail_table_columns)
                current_dtl_row[0] = atg_dtl_ref
                current_dtl_row[1] = atg_ref_header            

dtl_table:DataFrame = DataFrame(dtl_rows,columns=detail_table_columns)
dtl_allowance_table:DataFrame = DataFrame(dtl_allowance_rows,columns=detail_allowance_table_columns)
dtl_table.to_csv('Mitchell_EDI_880_DTL.csv',index=False)
dtl_allowance_table.to_csv("Mitchell_EDI_880_DTL_Allowances.csv",index=False)
