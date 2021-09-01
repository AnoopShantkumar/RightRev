import snowflake.connector
import time
import threading


ctx = snowflake.connector.connect(
    user='AnoopSR',
    password='Anoop@8197',
    account='ev02808.us-central1.gcp',
    warehouse='DEMO_WH',
    database='k_to_sf',
    schema='public')

cs = ctx.cursor()
"""sql = f"Create or replace STAGE DEMO1_DB.public.revContract_S3;"
cs.execute(sql)

print('Staging is created')


def Send_files_to_sf():
    sql = f"put file://C:/Users/Public/*.json @revContract_S3;"
    cs.execute(sql)
    print('file uploaded to the staging')"""

"""
def insert_file_into_table():
  sql = f"create or replace table v1(V)"
    cs.execute(sql)
    print('variant table is created')
    sql=f"copy into rev_Contract_raw (src) from (select * from @revcontract_s3/RC105.json.gz) file_format = (type = JSON);"
    cs.execute(sql)
    print('updating_rev_Contract_raw_tables')"""

def update_rc_header_table():
    sql = f"truncate table RC_header_table"
    cs.execute(sql)
    sql = f"insert into RC_header_table(RC_number, PO_number, PO_date, Contract_value, Billings, Bookings,Contract_assets, Planned_revenue, Recognized_revenue) select V: identifiers.revenue_contract_id::string,V: grouping_value.PoNumber::string,V: statuses.created_period::string,V: rc_metrics.amount[0]::string,V: revenue_contract.billings::string,0.0,0.0,0.0,0.0 from V1;"

    cs.execute(sql)
    print('RC_header_table updated')


def inserting_into_order_lines():
    sql= f"truncate table RC_order_lines_table"
    cs.execute(sql)
    sql = f"insert into RC_order_lines_table select distinct child.key as Order_line_Id,child.value:product_name::varchar as product_name,rel_act.value:created_on::date as Created_on,rel_act.value:quantity::varchar as quantity,rel_act.value:revenue_start_date::date as revenue_start_date,rel_act.value:revenue_end_date::date as revenue_end_date,rel_act.value:action::varchar as action,rel_act.value:amount::float as amount,'USD' from V1 p,lateral flatten(input =>parse_json(p.V:contract_details.order_lines), outer => true) children, lateral flatten(children.value) child, lateral flatten(child.value:release_actions) rel_act;"
    cs.execute(sql)
    print('RC_order_lines_table updated')

def inserting_into_Contract_raw():
    sql = f"truncate table rc_contractual_rev_table"
    cs.execute(sql)
    sql = f"insert into rc_contractual_rev_table select distinct p.V:identifiers.revenue_contract_id::string as Rev_Contract_ID,p.V:grouping_value.PoNumber::string as PO_number,p.V:rc_attributes.customer_name::string as customer,line_met_dim.key as Order_line_Id,line_met_dim.value:product_name::varchar as product_name ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.currency::varchar as currency,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.current_qtr_rev::float as current_qtr_rev,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.next_qtr_rev::float as next_qtr_rev ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.previous_qtr_rev::float as prev_qtr_rev,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.total_bookings::float as total_bookings,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.unplanned::float as unplanned ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.unposted::float as unposted,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ptd[0]::float as bookings_ptd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ptd[1]::float as opening_balance_ptd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ptd[2]::float as additions_ptd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ptd[3]::float as planned_ptd,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ptd[4]::float as recognized_ptd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ptd[5]::float as ending_balance_ptd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.qtd[0]::float as bookings_qtd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.qtd[1]::float as opening_balance_qtd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.qtd[2]::float as additions_qtd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.qtd[3]::float as planned_qtd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.qtd[4]::float as recognized_qtd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.qtd[5]::float as ending_balance_qtd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.utd[0]::float as bookings_utd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.utd[1]::float as opening_balance_utd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.utd[2]::float as additions_utd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.utd[3]::float as planned_utd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.utd[4]::float as recognized_utd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.utd[5]::float as ending_balance_utd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ytd[0]::float as bookings_ytd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ytd[1]::float as opening_balance_ytd,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ytd[2]::float as additions_ytd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ytd[3]::float as planned_ytd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ytd[4]::float as recognized_ytd ,line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency.ytd[5]::float as ending_balance_ytd from V1 p,lateral flatten(input => parse_json(p.V:contract_details.order_lines), outer => true) ordLine ,lateral flatten(ordLine.value) line_met_dim ,lateral flatten(line_met_dim.value:line_metric_dimensions.contractual_revenue.reporting_currency) repCy ;"
    cs.execute(sql)
    print('rev_Contract_raw updated')

i = 1
while  i == 1:
    user_input = input("Please enter the key")
    #if user_input == 'sfs':
        #Send_files_to_sf()
    #if user_input == 'ins':
        #insert_file_into_table()
    if user_input == 'rch':
        update_rc_header_table()
    elif user_input == 'rlo':
        inserting_into_order_lines()
    elif user_input == 'rcr':
        inserting_into_Contract_raw()
    else:
        continue
