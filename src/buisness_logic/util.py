import urllib.parse

def parse_urlstr(encoded_str):
      run=True
      normal_str=''
      while run:
            normal_str=urllib.parse.unquote_plus(encoded_str)
            if normal_str==encoded_str:
                  run=False
            else:
                  encoded_str=normal_str
      return normal_str

      