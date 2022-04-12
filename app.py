import streamlit as st
import numpy as np
import pandas as pd
import datetime
import time
import requests
import sqlalchemy
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from PIL import Image

st.set_page_config(page_title='Wallet Transaction Analysis', layout='wide')

def wallet_validator(inp):
    try:
        if len(inp) == 42 and int(inp, 16): #checking hexadecimal
            return True
        else:
            return False
    except:
        return False

#etherscan API key
my_key = st.secrets['my_key']

engine = sqlalchemy.create_engine(f"postgresql://{st.secrets['USER']}:{st.secrets['PASSWORD']}@{st.secrets['HOST']}:{st.secrets['PORT']}/{st.secrets['DBNAME']}")

if 'timeline' not in st.session_state:
    st.session_state.timeline = 'Outgoing'
    st.session_state.start = datetime.date(2022,4,1)
    st.session_state.end = datetime.date(2022,4,7)

col1, col2, col3 = st.columns(3)

with col1:
    st.write('#')
    st.write('#')
    st.write('#')

with col2:
    st.write('## Wallet Transaction Analysis App 🔎')

with col3:
    st.write('#')
    st.write('#')
    st.write('#')

colb1, colb3, colb2 = st.columns([10,1,10])

with colb1:
    
    st.text_input('Enter ETH wallet address here:', key='wallet_address')
    st.text('Last address submitted: ' + st.session_state['wallet_address'])
    wallet_address = st.session_state['wallet_address'].lower()
    
    if wallet_validator(wallet_address):

        st.session_state.start = st.date_input('Select starting date of desired time period:',
            value=st.session_state.start, min_value=datetime.date(2018,1,1), max_value=datetime.date.today() - datetime.timedelta(days=2))
        st.session_state.end = st.date_input('Select ending date of desired time period:',
            value=st.session_state.end, min_value=st.session_state.start + datetime.timedelta(days=1), max_value=datetime.date.today() - datetime.timedelta(days=1))


        #updating exchange rates table
        last_exchange_rate_date_info = pd.read_sql_query('SELECT date_id, ref_date FROM exchange_rates ORDER BY date_id DESC LIMIT 1;', engine, parse_dates=['ref_date']) 
        last_exchange_rate_id = last_exchange_rate_date_info['date_id'][0]
        last_exchange_rate_date = last_exchange_rate_date_info['ref_date'][0].to_pydatetime()
        last_exchange_rate_date = last_exchange_rate_date.date()
        
        rate_lis = []
        if st.session_state.end > last_exchange_rate_date:
            missing_dates = (st.session_state.end - last_exchange_rate_date).days
            for i in range(1, missing_dates+1):
                missing_day = (last_exchange_rate_date + datetime.timedelta(days=i))
                missing_rate = requests.get('https://api.coingecko.com/api/v3/coins/ethereum/history?',
                    params={'date':missing_day.strftime('%d-%m-%Y'), 'localization': 'false'}).json()['market_data']['current_price']['usd']
                rate_lis.append((last_exchange_rate_id + i, str(missing_day), missing_rate))
                
            pd.DataFrame(data=rate_lis, columns = ['date_id', 'ref_date', 'eth_usd_rate']).to_sql('exchange_rates', engine, if_exists='append', index=False, method = 'multi')
        
        ###################################

        #updating blocks table
        last_block_date_info = pd.read_sql_query('SELECT ref_date FROM blocks ORDER BY ref_date DESC LIMIT 1;', engine, parse_dates=['ref_date']) 
        last_block_date = last_block_date_info['ref_date'][0].to_pydatetime()
        last_block_date = last_block_date.date()
        
        block_lis = []
        if st.session_state.end > last_block_date:
            missing_dates = (st.session_state.end - last_block_date).days
            for i in range(1, missing_dates+1):
                missing_day = (last_block_date + datetime.timedelta(days=i))
                missing_day_timestamp = int(time.mktime(missing_day.timetuple()))
                missing_block_no = requests.get('https://api.etherscan.io/api?', params={'module':'block', 'action':'getblocknobytime', 'timestamp': missing_day_timestamp,
                    'closest': 'before', 'apikey':my_key}).json()['result']
                block_lis.append((missing_block_no, str(missing_day)))
            
            pd.DataFrame(data=block_lis, columns=['block_number', 'ref_date']).to_sql('blocks', engine, if_exists='append', index=False, method = 'multi')
        ###################################

        #check if we need to use API to collect transaction data or not
        unindexed_date_lis = []
        for i in range(0, (st.session_state.end - st.session_state.start).days + 1):
            unindexed_date_lis.append(st.session_state.start + datetime.timedelta(days=i))

        wallet_dict = pd.read_sql_query('SELECT wallet_id, wallet_address FROM wallets;', engine).set_index('wallet_address').T.to_dict('records')
        if len(wallet_dict) > 0:
            wallet_dict = wallet_dict[0]
        else:
            wallet_dict = {}

        try: #existing wallet
            wallet_id = wallet_dict[wallet_address]
            
            unindexed_date_lis = set(unindexed_date_lis) - set([i.date() for i in pd.read_sql_query(f'SELECT ref_date FROM data_index WHERE wallet_id = {wallet_id};',
                engine, parse_dates=['ref_date'])['ref_date'].dt.to_pydatetime()])
        
        except: #first-time wallet
            last_wallet_id = pd.read_sql_query('SELECT MAX(wallet_id) FROM wallets;', engine).iloc[0,0]

            if last_wallet_id is None:
                last_wallet_id = 0

            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(f"INSERT INTO wallets (wallet_id, wallet_address) VALUES ({last_wallet_id + 1}, '{wallet_address}');")
            
            wallet_id = pd.read_sql_query(f"SELECT wallet_id FROM wallets WHERE wallet_address = '{wallet_address}';", engine)['wallet_id'][0]
            wallet_dict[wallet_address] = wallet_id

        ###################################
        
        if len(unindexed_date_lis) > 0: #need to use transactions API
            #case 1: unindexed_data_lis is empty which means no API query needed and data already present
            #case 2: some data is missing which needs to be queried. Because overlaps might exist in data as same wallet is 
            #queried over different ranges, we will need to make sure that the sql update we do on the transactions table 
            #does not get an error because of repeated data and that only new data gets inserted

            min_unindexed_date = min(unindexed_date_lis)
            max_unindexed_date = max(unindexed_date_lis)

            if min_unindexed_date == max_unindexed_date: #to deal with the situation where unindexed_data_lis has only one element
                min_unindexed_date = min_unindexed_date - datetime.timedelta(days=1)
                #max_unindexed_date = max_unindexed_date + datetime.timedelta(days=1)

            start_block = pd.read_sql_query(f"SELECT block_number FROM blocks WHERE ref_date = '{str(min_unindexed_date)}';", engine)['block_number'][0]
            end_block = pd.read_sql_query(f"SELECT block_number FROM blocks WHERE ref_date = '{str(max_unindexed_date)}';", engine)['block_number'][0]

            results_list = requests.get('https://api.etherscan.io/api?', params={'module':'account', 'action':'txlist', 'address': wallet_address,
                'startblock': start_block, 'endblock': end_block, 'page': 1, 'offset': 10000, 'sort': 'desc', 'apikey':my_key}).json()['result']

            if len(results_list) == 10000:
                st.warning('The selected date range for this wallet is too large in terms of records requested for our free API key to handle. Please reload and select a smaller date range.')
                st.stop()

            elif len(results_list) > 0: #some data was retrieved which had quantity less than 10000
                
                last_transaction_id = pd.read_sql_query('SELECT MAX(transaction_id) FROM staging_transactions;', engine).iloc[0,0]

                if last_transaction_id is None:
                    last_transaction_id = 0

                res_lis = []
                update_wallet_lis = []
                for indx, res in enumerate(results_list):
                    
                    res['from'] = res['from'].lower()
                    res['to'] = res['to'].lower()

                    try:
                        from_id = wallet_dict[res['from']]
                    except:
                        wallet_dict[res['from']] = max(wallet_dict.values()) + 1
                        update_wallet_lis.append((wallet_dict[res['from']], res['from']))
                        from_id = wallet_dict[res['from']]
                    
                    try:
                        to_id = wallet_dict[res['to']]
                    except:
                        wallet_dict[res['to']] = max(wallet_dict.values()) + 1
                        update_wallet_lis.append((wallet_dict[res['to']], res['to']))
                        to_id = wallet_dict[res['to']]

                    tran_time = datetime.datetime.fromtimestamp(int(res['timeStamp']), tz=datetime.timezone.utc)

                    res_lis.append((last_transaction_id + indx + 1, res['hash'], int(res['blockNumber']), float(res['value'])/10**18, from_id, to_id, tran_time, float(res['gasPrice']), float(res['gasUsed'])))

                #send all data to staging table first
                transaction_df = pd.DataFrame(data=res_lis, columns=['transaction_id', 'transaction_hash', 'block_number', 'value_eth', 'from_wallet', 'to_wallet', 'transaction_time_utc', 'gas_price_wei', 'gas_used_wei'])
                transaction_df.to_sql('staging_transactions', engine, if_exists='replace', index=False, method = 'multi')
                
                #then move only those rows from staging_transactions that are not in transactions - this helps us avoid getting repeats in the transactions table
                with engine.connect() as conn:
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                    sql_stmt = f"INSERT INTO transactions (transaction_hash, block_number, value_eth, from_wallet, to_wallet, transaction_time_utc, gas_price_wei, gas_used_wei) \
                    SELECT s.transaction_hash, s.block_number, s.value_eth, s.from_wallet, s.to_wallet, s.transaction_time_utc, s.gas_price_wei, s.gas_used_wei \
                    FROM staging_transactions s \
                    ON CONFLICT DO NOTHING;"
                    #WHERE NOT EXISTS  \
                    #(SELECT 1 FROM transactions s \
                    #WHERE (s.transaction_hash IN (SELECT transaction_hash from t) AND t.from_wallet= s.from_wallet));"
                    conn.execute(sql_stmt)

                #update wallets
                pd.DataFrame(data=update_wallet_lis, columns = ['wallet_id', 'wallet_address']).to_sql('wallets', engine, if_exists='append', index=False, method = 'multi')

            #whether zero records were retrieved for the dates in unindexed_date_lis or not, we should update data_index
            last_record_id = pd.read_sql_query('SELECT MAX(record_id) FROM data_index;', engine).iloc[0,0]

            if last_record_id is None:
                last_record_id = 0

            update_record_lis = []
            for indx, item in enumerate(unindexed_date_lis):
                update_record_lis.append((last_record_id + indx + 1, wallet_id, str(item)))

            pd.DataFrame(data=update_record_lis, columns = ['record_id', 'wallet_id', 'ref_date']).to_sql('data_index', engine, if_exists='append', index=False, method = 'multi')

        
        ###################################
        
        #extract and combine data from different tables and display in optional container

        #incoming transactions 
        incoming_df = pd.read_sql_query(f"SELECT t.transaction_hash, t.block_number, t.value_eth, t.transaction_time_utc, w.wallet_address, t.gas_price_wei, t.gas_used_wei \
            FROM transactions AS t \
            JOIN wallets AS w \
            ON t.from_wallet = w.wallet_id \
            WHERE t.to_wallet = {wallet_id} AND \
            t.transaction_time_utc::date BETWEEN '{str(st.session_state.start)}' AND '{str(st.session_state.end)};'", #mismatch between UTC time and local time but not worth solving now
            engine, parse_dates=['transaction_time_utc'])
        
        rate_df = pd.read_sql_query(f"SELECT ref_date, eth_usd_rate \
            FROM exchange_rates;",
            engine, parse_dates=['ref_date'])

        rate_df.columns = ['transaction_date', 'eth_usd_rate']
        rate_df['transaction_date'] = pd.Series(rate_df['transaction_date'].dt.to_pydatetime()).apply(lambda x: x.date())

        incoming_df['transaction_fee_eth'] = incoming_df['gas_price_wei'] * incoming_df['gas_used_wei'] / 10**18
        incoming_df['transaction_date'] = pd.Series(incoming_df['transaction_time_utc'].dt.to_pydatetime()).apply(lambda x: x.date())
        incoming_df['transaction_time_utc'] = pd.Series(incoming_df['transaction_time_utc']).apply(lambda x: x.strftime('%Y-%m-%d %H:%M'))

        incoming_df = incoming_df.merge(rate_df, on='transaction_date', how='inner')
        incoming_df['historical_value_usd'] = incoming_df['value_eth'] * incoming_df['eth_usd_rate']
        
        current_rate = float(requests.get('https://api.etherscan.io/api?', params={'module':'stats', 'action':'ethprice', 'apikey':my_key}).json()['result']['ethusd'])
        col_name = 'current_value_usd @ ' + str(current_rate)
        incoming_df[col_name] = incoming_df['value_eth'] * current_rate
        incoming_df['transaction_fee_usd@current_rate'] = incoming_df['transaction_fee_eth'] * current_rate

        incoming_df = incoming_df.sort_values('transaction_time_utc', ascending=False, ignore_index=True)

        
        #outgoing transactions
        outgoing_df = pd.read_sql_query(f"SELECT t.transaction_hash, t.block_number, t.value_eth, t.transaction_time_utc, w.wallet_address, t.gas_price_wei, t.gas_used_wei \
            FROM transactions AS t \
            JOIN wallets AS w \
            ON t.to_wallet = w.wallet_id \
            WHERE t.from_wallet = {wallet_id} AND \
            t.transaction_time_utc::date BETWEEN '{str(st.session_state.start)}' AND '{str(st.session_state.end)}';", #mismatch between UTC time and local time but not worth solving
            engine, parse_dates=['transaction_time_utc'])

        outgoing_df['transaction_fee_eth'] = outgoing_df['gas_price_wei'] * outgoing_df['gas_used_wei'] / 10**18
        outgoing_df['transaction_date'] = pd.Series(outgoing_df['transaction_time_utc'].dt.to_pydatetime()).apply(lambda x: x.date())
        outgoing_df['transaction_time_utc'] = pd.Series(outgoing_df['transaction_time_utc']).apply(lambda x: x.strftime('%Y-%m-%d %H:%M'))

        outgoing_df = outgoing_df.merge(rate_df, on='transaction_date', how='inner')
        outgoing_df['historical_value_usd'] = outgoing_df['value_eth'] * outgoing_df['eth_usd_rate']
        
        outgoing_df[col_name] = outgoing_df['value_eth'] * current_rate
        outgoing_df['transaction_fee_usd@current_rate'] = outgoing_df['transaction_fee_eth'] * current_rate

        outgoing_df = outgoing_df.sort_values('transaction_time_utc', ascending=False, ignore_index=True)

        
        ###################################
        st.markdown("""---""")
        #summary and visualizations
        if len(incoming_df) > 0:
            st.markdown('**Total inward transfers:** ' + '{:,.2f}'.format(round(incoming_df['value_eth'].sum(), 2)) + 'ETH / ' + '{:,.2f}'.format(round(incoming_df[col_name].sum(), 2)) + 'USD')

            st.markdown('**Average inward transfer:** ' + '{:,.2f}'.format(round(incoming_df['value_eth'].sum()/len(incoming_df), 2)) + 'ETH / ' + '{:,.2f}'.format(round(incoming_df[col_name].sum()/len(incoming_df), 2))+'USD')
        else:
            st.write('No incoming transactions for the selected time period and wallet address.')

        if len(outgoing_df) > 0:   
            st.markdown('**Total outward transfers:** ' + '{:,.2f}'.format(round(outgoing_df['value_eth'].sum(), 2)) + 'ETH / ' + '{:,.2f}'.format(round(outgoing_df[col_name].sum(), 2))+'USD')

            st.markdown('**Average outward transfer:** ' + '{:,.2f}'.format(round(outgoing_df['value_eth'].sum()/len(outgoing_df), 2)) + 'ETH / ' + '{:,.2f}'.format(round(outgoing_df[col_name].sum()/len(outgoing_df), 2))+'USD')
        else:
            st.write('No outgoing transactions for the selected time period and wallet address.') 

        if len(incoming_df) > 0:
            incoming_largest = incoming_df.sort_values(by='value_eth', ascending=False).head(3).loc[:, ['wallet_address', 'value_eth']]
            st.write('**Largest incoming transactions:**')
            st.dataframe(incoming_largest.style.format(precision=3, thousands=','))

        if len(outgoing_df) > 0:
            outgoing_largest = outgoing_df.sort_values(by='value_eth', ascending=False).head(3).loc[:, ['wallet_address', 'value_eth']]
            st.write('**Largest outgoing transactions:**')
            st.dataframe(outgoing_largest.style.format(precision=3, thousands=','))
        
        #counterparty bar charts
        fig = make_subplots(rows=1, cols=2, subplot_titles=('Incoming', 'Outgoing'))

        incoming_grouped = incoming_df.groupby('wallet_address').agg({'value_eth': 'sum', 'transaction_hash': 'count'}).sort_values(by='value_eth', ascending=False).reset_index().head(5)
        incoming_grouped.columns = ['wallet_address', 'value_eth', 'transaction_count']
        fig.add_trace(go.Bar(x=incoming_grouped['wallet_address'], y=incoming_grouped['value_eth'], marker=dict(color="green"), showlegend=False), row=1, col=1) 
        fig.update_yaxes(title_text='Total amount transferred in ETH', row=1, col=1)
        
        outgoing_grouped = outgoing_df.groupby('wallet_address').agg({'value_eth': 'sum', 'transaction_hash': 'count'}).sort_values(by='value_eth', ascending=False).reset_index().head(5)
        outgoing_grouped.columns = ['wallet_address', 'value_eth', 'transaction_count']
        fig.add_trace(go.Bar(x=outgoing_grouped['wallet_address'], y=outgoing_grouped['value_eth'], marker=dict(color="crimson"), showlegend=False), row=1, col=2)
        
        fig.update_layout(title={'text':'<b>Largest counterparty wallets<b>', 'font': {'size': 30}, 'xanchor': 'center', 'x':0.4})
        
        fig.update_xaxes(
                tickangle = 20,
                tickfont = {"size": 10},
                title_font={'size':20, 'family':'verdana', 'color':'indigo'}
                )
        fig.update_yaxes(
                tickfont = {"size": 15},
                title_font={'size':20, 'family':'verdana', 'color':'indigo'}
                )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("""---""")

        #timeline charts
        st.write('##### Visualize incoming or outgoing transfers over time. Select:')
        st.session_state.timeline = st.radio('',('Incoming', 'Outgoing'), index=1)

        if st.session_state.timeline == 'Incoming':
            df_timeline = incoming_df.sort_values('transaction_date', ascending=False).groupby('transaction_date').agg({'value_eth': 'sum', 'transaction_hash': 'count'}).reset_index()
            df_timeline.columns = ['transaction_date', 'total_eth_transferred', 'transaction_count']
        elif st.session_state.timeline == 'Outgoing':
            df_timeline = outgoing_df.sort_values('transaction_date', ascending=False).groupby('transaction_date').agg({'value_eth': 'sum', 'transaction_hash': 'count'}).reset_index()
            df_timeline.columns = ['transaction_date', 'total_eth_transferred', 'transaction_count']

        fig2 = make_subplots(specs=[[{"secondary_y": True}]])

        fig2.add_trace(go.Scatter(x=df_timeline['transaction_date'], y=df_timeline['transaction_count'], name="transaction_count"), secondary_y=False,)

        fig2.add_trace(go.Scatter(x=df_timeline['transaction_date'], y=df_timeline['total_eth_transferred'], name="total_eth_transferred"),secondary_y=True,)

        fig2.update_layout(title={'text': '<b>ETH transfers over the time period<b>', 'font': {'size': 30}, 'xanchor': 'center', 'x':0.4})
        fig2.update_xaxes(title_text="Timeline")
        fig2.update_yaxes(title_text="transaction_count", secondary_y=False)
        fig2.update_yaxes(title_text="total_eth_transferred", secondary_y=True)

        st.plotly_chart(fig2, use_container_width=True)

        st.markdown("""---""")

        #sunburst
        incoming_grouped['direction'] = 'incoming transfers'
        outgoing_grouped['direction'] = 'outgoing transfers'

        sunburst_df = pd.concat([incoming_grouped, outgoing_grouped], ignore_index=True)
        sunburst_df = sunburst_df[sunburst_df['value_eth'] != 0]

        if len(sunburst_df) > 0:
            fig3 = px.sunburst(sunburst_df, path=['direction', 'wallet_address'], values='value_eth',
                        color='transaction_count',
                        color_continuous_scale='RdBu',
                        color_continuous_midpoint=np.average(sunburst_df['transaction_count']))

            fig3.update_layout(title={'text': '<b>ETH flows<b>', 'font': {'size': 30}, 'xanchor': 'center', 'x':0.45})
            st.plotly_chart(fig3, use_container_width=True)

        ###################################
    else:
        st.warning('Your input failed validation. Please check your wallet address input.')   
        
#################################################################################################
with colb2:
    #since the wallet address input might not pass validation, we can end up with no incoming_df and outgoing_df
    try: 
        if len(incoming_df) >= 20:
            st.write('**Last 20 incoming transactions:**')
            st.dataframe(incoming_df[['transaction_hash', 'block_number', 'transaction_time_utc', 'wallet_address', 'value_eth', col_name, 'historical_value_usd',
                'transaction_fee_eth', 'transaction_fee_usd@current_rate']].head(20).style.format({'block_number':'{:,.0f}', 'transaction_fee_eth':'{:e}', 'value_eth': '{:,.3f}', col_name: '{:,.2f}', 'historical_value_usd': '{:,.2f}', 'transaction_fee_usd@current_rate': '{:,.2f}'}))
        elif len(incoming_df) > 0:
            st.write(f'**Last {len(incoming_df)} incoming transactions:**')
            st.dataframe(incoming_df[['transaction_hash', 'block_number', 'transaction_time_utc', 'wallet_address', 'value_eth', col_name, 'historical_value_usd',
                'transaction_fee_eth', 'transaction_fee_usd@current_rate']].style.format({'block_number':'{:,.0f}', 'transaction_fee_eth':'{:e}', 'value_eth': '{:,.3f}', col_name: '{:,.2f}', 'historical_value_usd': '{:,.2f}', 'transaction_fee_usd@current_rate':'{:,.2f}'}))
        
        st.markdown("""---""")

        if len(outgoing_df) >= 20:
            st.write('**Last 20 outgoing transactions:**')
            st.dataframe(outgoing_df[['transaction_hash', 'block_number', 'transaction_time_utc', 'wallet_address', 'value_eth', col_name, 'historical_value_usd',
                'transaction_fee_eth', 'transaction_fee_usd@current_rate']].head(20).style.format({'block_number':'{:,.0f}', 'transaction_fee_eth':'{:e}', 'value_eth': '{:,.3f}', col_name: '{:,.2f}', 'historical_value_usd': '{:,.2f}', 'transaction_fee_usd@current_rate': '{:,.2f}'}))
        elif len(outgoing_df) > 0:
            st.write(f'**Last {len(outgoing_df)} outgoing transactions:**')
            st.dataframe(outgoing_df[['transaction_hash', 'block_number', 'transaction_time_utc', 'wallet_address', 'value_eth', col_name, 'historical_value_usd',
                'transaction_fee_eth', 'transaction_fee_usd@current_rate']].style.format({'block_number':'{:,.0f}', 'transaction_fee_eth':'{:e}', 'value_eth': '{:,.3f}', col_name: '{:,.2f}', 'historical_value_usd': '{:,.2f}', 'transaction_fee_usd@current_rate': '{:,.2f}'}))
        
        st.markdown("""---""")
    except:
        pass


    st.write('### Overall structure:')
    st.write("""This app is created with Streamlit which is a Python-based web framework.\n
Wallet transactions are collected from the EtherScan API. ETH/USD rates are collected from the CoinGecko API.\n
All of the data that is extracted from the different APIs is stored in a PostgreSQL database on AWS. Data as shown on a wallet's home page on EtherScan is not available from a single API endpoint.\n
The different API endpoints used, in addition to my mental model of an ETH wallet transaction, influenced my design of the database.""")

    image = Image.open('db_schema.png')
    st.image(image, caption='Database schema')

    st.write("""This database is always queried first whenever a new query is submitted by the app user in order to avoid unnecessary requests to the API endpoints.\n
Since the ETL process is triggered by the user submitting a query and is not run on a schedule, using an orchestrator like Airflow isn't appropriate.\n\n
To do:
- Figure out how to scrape (without getting IP-blocked) wallet name tags from EtherScan since they aren't covered by the API.
- Consider paid API services for more granular data.
- Do more code testing and improve error handling.""")

engine.dispose()
