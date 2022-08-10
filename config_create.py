import configparser

config = configparser.ConfigParser(interpolation=None)

config['twitter'] = {
  'api_key': 'vvGdOKUfvU711lG9JDlO5FpSQ',
  'api_key_secret': 'fMrpULModBRnAtEGrQ9yZKkkjuqflxJ4HL6YpRJ7f6UbsNxnSy',
  'bearer_token': "AAAAAAAAAAAAAAAAAAAAAJs2fgEAAAAAWvaPqjgxjVEEAqvBUJj8bxf9JnI%3DN8Sbth2N3XbaelNTAX2aIMzRn0AGS0GnJ9nWGMosd3TGADatDQ",
  'access_token': '1554746939740135424-cZoA5YuleTWk5lVg1uatlbVQNU663P',
  'access_token_secret': '8jkjpvnb5WB7rRHSWkrGm3WF4C3PWe2T1fqqnirbaBxp4'
}

with open('./config.ini', 'w') as f:
  config.write(f)