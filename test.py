# -*- coding: utf-8 -*-

import string


if __name__ == '__main__':
    str = "1448,2060,2072,2128,2280,2344,2364,2436,2652,2664,2804,2904,2924,3176,3348,3720,3852,3960,4148,4160,4224,4416,4724,4988,5060,5072,5332,5352,5536,5564,5732,5796,6176,6256,6356,6424,6464,6568,6884,7000,7088,7516,7700,7884,7896,7948,8080,8084,8180,8576,8744,"
    ps = str.split(',')
    for p in ps:
        b = "goog"
        t = p.strip()
        if t:
            a = int(t)
            print a
    print b

