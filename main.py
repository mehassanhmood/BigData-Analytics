from dotenv import dotenv_values

from src.common.fmp import FmpData

if __name__ == '__main__':
    config = dotenv_values('.env')

    # all other configurations
    sec = ["BT", "ES", "GC", "ZN", "SI", "LS", "NG", "CL", "DX"]
    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL', 'CVX', 'XOM',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]

    crypto_symbols = ['BTCUSD']
    start_data = '2004-01-01'

    # intialize fmp api
    fmp_api = FmpData(api_key=config['fmp_key'],
                      maria_conf={
                            'user': config['maria_user'],
                            'password': config['maria_pwd'],
                            'host': config['maria_host'],
                            'port': config['maria_port'],
                            'database': config['maria_database'],
                            'driver': config['maria_driver'],
                      },)

    # get data from sec and save them in tables (Tested OK)
    for item in sec:
        # set the write table
        fmp_api.table_name = item
        fmp_api.get_fmp_cot(item, start_date=start_data)

    # get historical full price and save it in table -> btc_data (not tested)
    fmp_api.table_name = 'btc_data'
    for symbol in crypto_symbols:
        fmp_api.get_historical_full_price(symbol=symbol, from_date=start_data)

    # get quarter income statement for all stocks in list (not tested)
    for symbol in stock_symbols:
        # set write table
        fmp_api.table_name = symbol
        fmp_api.get_income_stmt(symbol=symbol, period='quarter')
