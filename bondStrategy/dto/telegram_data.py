class TelegramData:
    def __init__(self, user_id, user_name, security_name, security_code, price, status=None, ratio=None):
        self.user_id = user_id
        self.user_name = user_name
        self.security_name = security_name
        self.security_code = security_code
        self.price = price
        self.status = status
        self.ratio = ratio


class TelegramDatas:
    def __init__(self):
        self.telegram_data_lists = []

    def add_telegram_data(self, security_list):
        self.telegram_data_lists.append(security_list)

    def __iter__(self):
        return iter(self.telegram_data_lists)
