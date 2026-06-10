#!/usr/bin/env python3
"""Patch question_bank_30p_30tc.json — remove exit(), fix sys import, fix TLE-prone solutions."""
import json

bank = json.load(open('question_bank_30p_30tc.json'))

FIXED = {

"trapping_rain_water": """
n=int(input())
h=list(map(int,input().split())) if n>0 else []
if n<3:
    print(0)
else:
    l,r=0,n-1;lm=h[0];rm=h[-1];res=0
    while l<r:
        if lm<=rm:
            l+=1;lm=max(lm,h[l]);res+=lm-h[l]
        else:
            r-=1;rm=max(rm,h[r]);res+=rm-h[r]
    print(res)
""".strip(),

"histogram_largest_rectangle": """
n=int(input())
h=list(map(int,input().split())) if n>0 else []
st=[];mx=0
for i,v in enumerate(h+[0]):
    while st and h[st[-1]]>v:
        ht=h[st.pop()];w=i if not st else i-st[-1]-1;mx=max(mx,ht*w)
    st.append(i)
print(mx)
""".strip(),

"decode_ways": """
s=input().strip()
if not s or s[0]=='0':
    print(0)
else:
    dp=[0]*(len(s)+1);dp[0]=dp[1]=1
    for i in range(2,len(s)+1):
        if s[i-1]!='0':dp[i]+=dp[i-1]
        if 10<=int(s[i-2:i])<=26:dp[i]+=dp[i-2]
    print(dp[len(s)])
""".strip(),

"edit_distance": """
s=input();t=input()
dp=list(range(len(t)+1))
for i in range(1,len(s)+1):
    prev=dp[0];dp[0]=i
    for j in range(1,len(t)+1):
        tmp=dp[j];dp[j]=(prev if s[i-1]==t[j-1] else 1+min(prev,dp[j],dp[j-1]));prev=tmp
print(dp[len(t)])
""".strip(),

"distinct_subsequences": """
s=input();t=input()
dp=[0]*(len(t)+1);dp[0]=1
for c in s:
    for j in range(len(t),0,-1):
        if c==t[j-1]:dp[j]+=dp[j-1]
print(dp[len(t)])
""".strip(),

"palindrome_min_cuts": """
s=input();n=len(s)
if n==0:
    print(0)
else:
    ip=[[False]*n for _ in range(n)]
    for i in range(n):ip[i][i]=True
    for i in range(n-1):ip[i][i+1]=(s[i]==s[i+1])
    for l in range(3,n+1):
        for i in range(n-l+1):
            j=i+l-1;ip[i][j]=ip[i+1][j-1] and s[i]==s[j]
    cuts=list(range(-1,n))
    for i in range(n):
        if ip[0][i]:cuts[i+1]=0
        else:
            for j in range(i):
                if ip[j+1][i]:cuts[i+1]=min(cuts[i+1],cuts[j+1]+1)
    print(cuts[n])
""".strip(),

"burst_balloons": """
n=int(input());nums=list(map(int,input().split()))
a=[1]+nums+[1];sz=len(a)
dp=[[0]*sz for _ in range(sz)]
for l in range(2,sz):
    for i in range(sz-l):
        j=i+l
        for k in range(i+1,j):
            dp[i][j]=max(dp[i][j],a[i]*a[k]*a[j]+dp[i][k]+dp[k][j])
print(dp[0][sz-1])
""".strip(),

"word_break": """
w=input().strip();n=int(input())
d=set(input().strip() for _ in range(n))
dp=[False]*(len(w)+1);dp[0]=True
for i in range(1,len(w)+1):
    for j in range(i):
        if dp[j] and w[j:i] in d:dp[i]=True;break
print(dp[len(w)])
""".strip(),

"min_jumps": """
n=int(input());nums=list(map(int,input().split()))
if n<=1:
    print(0)
else:
    j=0;ce=0;far=0
    for i in range(n-1):
        far=max(far,i+nums[i])
        if i==ce:j+=1;ce=far
        if ce>=n-1:break
    print(j)
""".strip(),

"first_missing_positive": """
n=int(input());a=list(map(int,input().split()))
for i in range(n):
    while 1<=a[i]<=n and a[a[i]-1]!=a[i]:a[a[i]-1],a[i]=a[i],a[a[i]-1]
ans=n+1
for i in range(n):
    if a[i]!=i+1:ans=i+1;break
print(ans)
""".strip(),

"longest_consecutive_sequence": """
n=int(input());nums=list(map(int,input().split()))
s=set(nums);best=0
for v in s:
    if v-1 not in s:
        cur=v;st=1
        while cur+1 in s:cur+=1;st+=1
        best=max(best,st)
print(best)
""".strip(),

"sliding_window_maximum": """
from collections import deque
n=int(input());nums=list(map(int,input().split()));k=int(input())
dq=deque();res=[]
for i,v in enumerate(nums):
    while dq and dq[0]<i-k+1:dq.popleft()
    while dq and nums[dq[-1]]<v:dq.pop()
    dq.append(i)
    if i>=k-1:res.append(nums[dq[0]])
print(*res)
""".strip(),

"n_queens_count": """
n=int(input())
def bt(r,c,d1,d2):
    if r==n:return 1
    cnt=0
    for col in range(n):
        if col in c or r-col in d1 or r+col in d2:continue
        c.add(col);d1.add(r-col);d2.add(r+col)
        cnt+=bt(r+1,c,d1,d2)
        c.remove(col);d1.remove(r-col);d2.remove(r+col)
    return cnt
print(bt(0,set(),set(),set()))
""".strip(),

"count_inversions": """
n=int(input());arr=list(map(int,input().split()))
def mc(a):
    if len(a)<=1:return a,0
    m=len(a)//2;l,lc=mc(a[:m]);r,rc=mc(a[m:])
    merged=[];cnt=lc+rc;i=j=0
    while i<len(l) and j<len(r):
        if l[i]<=r[j]:merged.append(l[i]);i+=1
        else:merged.append(r[j]);j+=1;cnt+=len(l)-i
    merged+=l[i:];merged+=r[j:]
    return merged,cnt
print(mc(arr)[1])
""".strip(),

"matrix_chain_multiplication": """
n=int(input());dims=list(map(int,input().split()))
dp=[[0]*n for _ in range(n)]
for l in range(2,n+1):
    for i in range(n-l+1):
        j=i+l-1;dp[i][j]=10**18
        for k in range(i,j):
            c=dp[i][k]+dp[k+1][j]+dims[i]*dims[k+1]*dims[j+1]
            dp[i][j]=min(dp[i][j],c)
print(dp[0][n-1])
""".strip(),

"egg_drop_minimum_trials": """
k,n=map(int,input().split())
if k==1:
    print(n)
elif n<=1:
    print(n)
else:
    dp=[[0]*(n+1) for _ in range(k+1)];m=0
    while dp[k][m]<n:
        m+=1
        for i in range(1,k+1):dp[i][m]=dp[i-1][m-1]+dp[i][m-1]+1
    print(m)
""".strip(),

"painters_partition": """
k=int(input());n=int(input());planks=list(map(int,input().split()))
def ok(mid):
    p=1;c=0
    for x in planks:
        if x>mid:return False
        if c+x>mid:p+=1;c=x
        else:c+=x
    return p<=k
lo,hi=max(planks),sum(planks)
while lo<hi:
    mid=(lo+hi)//2
    if ok(mid):hi=mid
    else:lo=mid+1
print(lo)
""".strip(),

"longest_palindromic_substring_length": """
s=input().strip()
if not s:
    print(0)
else:
    t='#'+'#'.join(s)+'#';nt=len(t);p=[0]*nt;c=r=0
    for i in range(nt):
        m=2*c-i
        if i<r:p[i]=min(r-i,p[m])
        while i+p[i]+1<nt and i-p[i]-1>=0 and t[i+p[i]+1]==t[i-p[i]-1]:p[i]+=1
        if i+p[i]>r:c,r=i,i+p[i]
    print(max(p))
""".strip(),

"k_transactions_max_profit": """
k=int(input());n=int(input());prices=list(map(int,input().split()))
if n==0 or k==0:
    print(0)
elif k>=n//2:
    print(sum(max(0,prices[i]-prices[i-1]) for i in range(1,n)))
else:
    dp=[[0]*n for _ in range(k+1)]
    for t in range(1,k+1):
        mx=-prices[0]
        for d in range(1,n):
            dp[t][d]=max(dp[t][d-1],prices[d]+mx);mx=max(mx,dp[t-1][d]-prices[d])
    print(dp[k][n-1])
""".strip(),

"maximum_sum_submatrix": """
rows,cols=map(int,input().split())
mat=[list(map(int,input().split())) for _ in range(rows)]
res=-10**9
for l in range(cols):
    rs=[0]*rows
    for r in range(l,cols):
        for i in range(rows):rs[i]+=mat[i][r]
        cur=rs[0];best=rs[0]
        for v in rs[1:]:cur=max(v,cur+v);best=max(best,cur)
        res=max(res,best)
print(res)
""".strip(),

"word_ladder_length": """
from collections import deque
beg=input().strip();end=input().strip();n=int(input())
wl=set(input().strip() for _ in range(n))
if end not in wl:
    print(0)
else:
    q=deque([(beg,1)]);vis={beg};ans=0
    while q:
        w,d=q.popleft()
        done=False
        for i in range(len(w)):
            for c in 'abcdefghijklmnopqrstuvwxyz':
                nw=w[:i]+c+w[i+1:]
                if nw==end:ans=d+1;done=True;break
                if nw in wl and nw not in vis:vis.add(nw);q.append((nw,d+1))
            if done:break
        if done:break
    print(ans)
""".strip(),

"optimal_bst_minimum_cost": """
n=int(input());freq=list(map(int,input().split()))
pre=[0]*(n+1)
for i in range(n):pre[i+1]=pre[i]+freq[i]
def rs(i,j):return pre[j+1]-pre[i]
dp=[[0]*n for _ in range(n)]
for i in range(n):dp[i][i]=freq[i]
for l in range(2,n+1):
    for i in range(n-l+1):
        j=i+l-1;dp[i][j]=10**18
        for r in range(i,j+1):
            lv=dp[i][r-1] if r>i else 0;rv=dp[r+1][j] if r<j else 0
            dp[i][j]=min(dp[i][j],lv+rv+rs(i,j))
print(dp[0][n-1])
""".strip(),

"boolean_parenthesization": """
expr=input().strip()
sym=expr[::2];ops=expr[1::2];n=len(sym)
T=[[0]*n for _ in range(n)];F=[[0]*n for _ in range(n)]
for i in range(n):T[i][i]=sym[i]=='T';F[i][i]=sym[i]=='F'
for l in range(2,n+1):
    for i in range(n-l+1):
        j=i+l-1
        for k in range(i,j):
            op=ops[k];lt,lf=T[i][k],F[i][k];rt,rf=T[k+1][j],F[k+1][j]
            if op=='&':T[i][j]+=lt*rt;F[i][j]+=lt*rf+lf*rt+lf*rf
            elif op=='|':T[i][j]+=lt*rt+lt*rf+lf*rt;F[i][j]+=lf*rf
            elif op=='^':T[i][j]+=lt*rf+lf*rt;F[i][j]+=lt*rt+lf*rf
print(T[0][n-1]%1000)
""".strip(),

"largest_divisible_subset_size": """
n=int(input());nums=list(map(int,input().split()))
if not nums:
    print(0)
else:
    nums.sort();dp=[1]*n
    for i in range(1,n):
        for j in range(i):
            if nums[i]%nums[j]==0:dp[i]=max(dp[i],dp[j]+1)
    print(max(dp))
""".strip(),

"count_smaller_numbers_after_self": """
n=int(input());nums=list(map(int,input().split()))
res=[0]*n
def mc(idx):
    if len(idx)<=1:return idx
    m=len(idx)//2;l=mc(idx[:m]);r=mc(idx[m:])
    merged=[];i=j=0;rs=0
    while i<len(l) and j<len(r):
        if nums[l[i]]>nums[r[j]]:rs+=1;merged.append(r[j]);j+=1
        else:res[l[i]]+=rs;merged.append(l[i]);i+=1
    while i<len(l):res[l[i]]+=rs;merged.append(l[i]);i+=1
    merged+=r[j:];return merged
mc(list(range(n)));print(*res)
""".strip(),

"minimum_cost_to_cut_stick": """
n=int(input());m=int(input())
cuts=sorted(map(int,input().split()))
c=[0]+list(cuts)+[n];sz=len(c)
dp=[[0]*sz for _ in range(sz)]
for l in range(2,sz):
    for i in range(sz-l):
        j=i+l;dp[i][j]=10**9
        for k in range(i+1,j):dp[i][j]=min(dp[i][j],c[j]-c[i]+dp[i][k]+dp[k][j])
print(dp[0][sz-1])
""".strip(),

"wildcard_matching": """
s=input();p=input()
m,n=len(s),len(p);dp=[[False]*(n+1) for _ in range(m+1)];dp[0][0]=True
for j in range(1,n+1):
    if p[j-1]=='*':dp[0][j]=dp[0][j-1]
for i in range(1,m+1):
    for j in range(1,n+1):
        if p[j-1]=='*':dp[i][j]=dp[i-1][j] or dp[i][j-1]
        elif p[j-1]=='?' or p[j-1]==s[i-1]:dp[i][j]=dp[i-1][j-1]
print(dp[m][n])
""".strip(),

"regular_expression_matching": """
s=input();p=input()
m,n=len(s),len(p);dp=[[False]*(n+1) for _ in range(m+1)];dp[0][0]=True
for j in range(1,n+1):
    if p[j-1]=='*':dp[0][j]=dp[0][j-2]
for i in range(1,m+1):
    for j in range(1,n+1):
        if p[j-1]=='*':dp[i][j]=dp[i][j-2] or (dp[i-1][j] and (p[j-2]=='.' or p[j-2]==s[i-1]))
        elif p[j-1]=='.' or p[j-1]==s[i-1]:dp[i][j]=dp[i-1][j-1]
print(dp[m][n])
""".strip(),

}

# edit_distance_v2 and palindrome_min_cuts_v2 share solutions with originals
FIXED["edit_distance_v2"]       = FIXED["edit_distance"]
FIXED["palindrome_min_cuts_v2"] = FIXED["palindrome_min_cuts"]

patched = 0
for prob in bank['problems']:
    pid = prob['id']
    if pid in FIXED:
        old = prob['solutions'][0]['source_code']
        new = FIXED[pid]
        if old != new:
            prob['solutions'][0]['source_code'] = new
            print(f"  PATCHED  {pid}")
            patched += 1
        else:
            print(f"  OK       {pid}")
    else:
        print(f"  SKIP     {pid}")

with open('question_bank_30p_30tc.json', 'w') as f:
    json.dump(bank, f, indent=2)

print(f"\nPatched {patched} solutions → question_bank_30p_30tc.json")
