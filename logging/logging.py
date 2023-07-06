import logging
import re

# 로거 만드는 표준 프로세스 -> 1) getLogger 2) setLevel(logging.DEBUG) 3) formatter 4) FileHandler 5) addHandler(formatter)
# 모듈별 로그 관리 위해서는 모듈별로 로거 따로 지정 필요 (안그러면 상위 모듈에 있는 로거가 override)

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG) # minimum level that logs

f = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s)- %(message)s')
fh = logging.FileHandler('logged.log', mode = 'w', encoding = 'utf-8')
fh.setFormatter(f)

logger.addHandler(fh)


def main(name):
    
    pattern = re.compile('^[a-dA-D]')

    if len(name) > 5:
        logger.error('too long names')

    elif re.search(pattern, name):
        logger.warning('name starts with a to d or name contains numbers')

    else:
        print(name)
        logger.info('complete')

if __name__ == '__main__':
    main('1adfss')
    main('tyyyg')
    main('dgss')
    main('ujl')