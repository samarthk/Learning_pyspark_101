a= [1,3,6,4,1,2]
#a= [1,3,2]

print a
len(a)
smint =0
a.sort()

print a
b=0
for x in a:
    if b!=len(a)-1:
        b = b + 1
    print a[b]
    if x+1< a[b]:
        smint=x+1

    #print('X = ', x)


print('X = ', smint)

