import yfinance as yf
from datetime import datetime, timedelta


class StockYieldCalculator:
    def __init__(self, stock_symbol):
        self.stock_symbol = stock_symbol

    def fetch_stock_price(self):
        """
        Fetch stock price from a year ago and today's date using raw closing prices.
        """
        # Get today's date
        today = datetime.now()
        # Get the date one year ago
        one_year_ago = today - timedelta(days=365)

        # Download historical data for the stock (one year ago to today)
        stock_data = yf.download(
            self.stock_symbol,
            start=one_year_ago.strftime("%Y-%m-%d"),
            end=today.strftime("%Y-%m-%d"),
            auto_adjust=False,
        )

        if stock_data.empty:
            return None, None

        # Get the stock price closest to a year ago (raw closing price)
        stock_price_year_ago = stock_data.iloc[0]["Close"]
        # Get the latest stock price (today's closing price)
        stock_price_today = stock_data.iloc[-1]["Close"]

        return stock_price_year_ago, stock_price_today

    def calculate_yield(self):
        """
        Calculate the stock yield percentage based on raw closing prices.
        """
        # Fetch stock prices from a year ago and today
        stock_price_year_ago, stock_price_today = self.fetch_stock_price()

        if stock_price_year_ago is None or stock_price_today is None:
            print(f"Unable to fetch data for {self.stock_symbol}.")
            return None

        # Calculate the yield percentage
        yield_percentage = ((stock_price_today - stock_price_year_ago) / stock_price_year_ago) * 100

        return yield_percentage


# Example usage
if __name__ == "__main__":
    stock_symbol = "NVDA"  # Replace with your stock symbol
    stock_calculator = StockYieldCalculator(stock_symbol)

    yield_percentage = stock_calculator.calculate_yield()

    if yield_percentage is not None:
        print(f"The yield of {stock_symbol} over the past year is: {yield_percentage:.2f}%")
    else:
        print(f"Could not calculate the yield for {stock_symbol}.")
