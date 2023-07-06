class AccountData:
    def __init__(self, company_uid, company_pin, account_id, uid, account_number, risk_score,
                 started_date, latest_deposited_at=None):
        self.csNo = company_uid
        self.pinNo = company_pin
        self.account_id = account_id
        self.uid = uid
        self.account_number = account_number  # 계좌번호 Encoding
        self.risk_score = risk_score
        self.started_date = started_date
        self.latest_deposited_at = latest_deposited_at
        self.evaluation_balance = None  # 주식 + 채권 + D+2 평가액
        self.total_evaluation_balance = None    # NPV - MP 종목 아닌 것들 뺀 평가액
        self.etf_amount = None
