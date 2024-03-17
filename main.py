from src.CryptoData import crypto_data
from src.CotData import cot_report
from src.FundamentalData import fundamental_data
from src.StockTrading import stock_trading
from src.StockNews import stock_news

if __name__ == "__main__":
    cot_report()
    crypto_data()
    fundamental_data()
    stock_trading()
    stock_news()