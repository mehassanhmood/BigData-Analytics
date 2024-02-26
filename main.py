from dotenv import dotenv_values

from src.common.fmp import FmpData

if __name__ == '__main__':
    config = dotenv_values('.env')
    sec = ["BT", "ES", "GC", "ZN", "SI", "LS", "NG", "CL", "DX"]
    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL', 'CVX', 'XOM',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]

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

    for item in sec:
        # set the write table
        fmp_api.table_name = item
        fmp_api.get_fmp_cot(item, start_date='2004-01-01')
        break

