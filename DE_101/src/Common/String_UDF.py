
def to_camelcase(in_string):
    final=''
    if in_string is not None:
        in_string=in_string.lower()
    else:
        return None
    for i in xrange(len(in_string)):
        item =in_string[i]
        if (in_string[i-1]==" " or in_string[i-1]=="_" or i==0):
            item=item.upper()

        final=final+item
    return final

if __name__ == '__main__':
    print(to_camelcase("Sammy KumadDSSar_xx"))
