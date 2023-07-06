import query
import csv
from unhandle import GlobalException
from database import Database


class Portfolio:
    def __init__(self):
        self.db = Database()

    def soft_delete(self, security_codes):
        # Soft delete all security codes with status "Buy"
        self.db.cur.execute("UPDATE iruda_trade.bond_portfolio SET deleted_at=Now() WHERE status='매수' AND deleted_at IS NULL")

        # Soft delete security codes with status "Sell" if the new CSV has the same security code with status "Buy"
        for code in security_codes:
            if code['status'] == "매수":
                self.db.cur.execute("UPDATE iruda_trade.bond_portfolio SET deleted_at=Now() WHERE security_code=%s AND status='매도' AND deleted_at IS NULL", (code['security_code'],))

    def update_portfolio(self, security_codes):
        self.db.connect_db()
        self.soft_delete(security_codes)
        self.db.con.commit()
        self.db.disconnect_db()

    def save_csv_to_mysql(self, file_path):
        security_codes = []
        with open(file_path, 'r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            next(csvreader)  # Skip header row

            for row in csvreader:
                if all(x.strip() == '' for x in row):
                    break

                symbol, name, remaining_days, start_date, end_date, interest_rate, average_price, \
                issued_amount, grade, ratio, status, price, extra = row[:13]
                security_codes.append({'security_code': symbol, 'status': status})

        self.update_portfolio(security_codes)  # Soft delete

        try:
            self.db.connect_db()
            with open(file_path, 'r') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',')
                next(csvreader)  # Skip header row

                for row in csvreader:
                    if all(x.strip() == '' for x in row):
                        break

                    symbol, name, remaining_days, start_date, end_date, interest_rate, average_price, \
                    issued_amount, grade, ratio, status, price, extra = row[:13]
                    symbol = symbol.strip()
                    name = name.strip()
                    ratio = float(ratio.strip('%')) / 100 if ratio else None
                    interest_rate = float(interest_rate) if interest_rate.strip() != '' else 0.0
                    price = float(price) if price else None
                    status = status if status.strip() else None
                    insert_csv = query.insert_csv()
                    table_field = (
                        symbol, name, remaining_days, start_date, end_date, interest_rate, average_price,
                        issued_amount, grade, ratio, status, price, extra
                    )
                    self.db.cur.execute(insert_csv, table_field)

            self.db.con.commit()
            print("한투용 포트폴리오 DB 저장 성공")
        except Exception as e:
            self.db.con.rollback()
            raise GlobalException(f"Error inserting CSV data to database: {e}")
        finally:
            self.db.disconnect_db()


if __name__ == "__main__":
    portfolio = Portfolio()
    # TODO file updated daily/monthly/quarterly...(once per period)
    portfolio.save_csv_to_mysql("/Users/jeong-yeongmin/Downloads/업라이즈_자문지양식_0531.csv")
