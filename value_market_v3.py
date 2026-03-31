import yfinance as yf
import sqlite3 as sql
import pandas as pd


def create_database():
    conn = sql.connect("Marketbo.db")
    conn.close()

def save_to_sql(df, table_name="market_history"):
    conn = sql.connect("Marketbo.db")
 
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.close()

def check_market_alerts(symbol, data, threshold):
  
    last_change = data['earning_percent'].iloc[-1]
    
    if abs(last_change) >= threshold:
        alert_type = "⚠️ WARNING" if last_change < 0 else "🚀 GROWTH"
   
        print(f"[{alert_type}] {symbol}: Detected movement of {last_change:.2f}%")
        return True
    return False


if __name__ == "__main__":
    print("--- System starting: Data Engineering Phase ---")
    create_database()

   
    alert_threshold = 2.0
    tickers_interes = ['SPY', 'TSLA', 'MSFT', 'AAPL', 'BTC-USD', 'META', 'BRK-B']

   
    global_green_days = 0
    global_red_days = 0
    global_positive_results = 0
    global_negative_results = 0

    print("Downloading and processing data...")

    for symbol in tickers_interes:
        print(f"\n> Analyzing: {symbol}")

        try:
         
            stock = yf.Ticker(symbol)
            stock_data = stock.history(period="2y")
            
            if stock_data.empty:
                print(f"No data found for {symbol}")
                continue

            stock_data["ticker"] = symbol
            stock_data["good_days"] = stock_data["Close"] > stock_data["Open"]
            stock_data["bad_days"] = stock_data["Close"] < stock_data["Open"]
            stock_data["earning_percent"] = ((stock_data["Close"] - stock_data["Open"]) / stock_data["Open"]) * 100
            stock_data["positive_result"] = stock_data["earning_percent"] > 0
            stock_data["negative_result"] = stock_data["earning_percent"] < 0

          
            current_green = stock_data["good_days"].sum()
            current_red = stock_data["bad_days"].sum()
            current_pos = stock_data["positive_result"].sum()
            current_neg = stock_data["negative_result"].sum()

        
            global_green_days += current_green
            global_red_days += current_red
            global_positive_results += current_pos
            global_negative_results += current_neg

      
            check_market_alerts(symbol, stock_data, alert_threshold)

     
            save_to_sql(stock_data)
            print(f"Successfully processed and saved {symbol}")

        except Exception as e:
            print(f"An unexpected error occurred with {symbol}: {e}")
            continue


    print("\n" + "="*40)
    print("📊 GLOBAL BOT REPORT (PORTFOLIO)")
    print(f"Total Green Days: {global_green_days}")
    print(f"Total Red Days: {global_red_days}")
    print(f"Total Positive Results: {global_positive_results}")
    print("="*40)

    print("\nGenerating consolidated Excel report...")
    try:
        conn = sql.connect("Marketbo.db")
        master_df = pd.read_sql("SELECT * FROM market_history", conn)
        conn.close()
        
        master_df.to_excel("stock_report.xlsx", index=False)
        print("Process completed! Check your 'stock_report.xlsx'.")
    except Exception as e:
        print(f"Error generating Excel: {e}")


    


     




