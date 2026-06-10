#!/usr/bin/env python3
"""
Generate question_bank_30p_30tc_multilang.json
Takes the Python-only fast_approach bank and adds C, C++, Java variants.
Each problem becomes 4 entries (python, c, cpp, java) with identical test cases.
Output: 120 problems total.
"""
import json, copy

SRC = "question_bank_30p_30tc_fast_approach.json"
DST = "question_bank_30p_30tc_multilang.json"

# ── Solutions: keyed by problem id, then language ────────────────────────────
# C/C++: student writes int main() — harness renames it via #define main student_stdio_main
# Java:  student writes static methods including main(String[]) — harness wraps in static class Student
# java.util.* is now imported in the harness so Scanner, ArrayList, HashMap etc. work directly.

SOLUTIONS = {

# ════════════════════════════════════════════════════════════════════════════
"trapping_rain_water": {
"c": r"""
#include <stdio.h>
int main() {
    int n; scanf("%d", &n);
    if (n == 0) { printf("0\n"); return 0; }
    int h[100001];
    for (int i = 0; i < n; i++) scanf("%d", &h[i]);
    if (n < 3) { printf("0\n"); return 0; }
    int l=0, r=n-1, lm=h[0], rm=h[n-1], res=0;
    while (l < r) {
        if (lm <= rm) { l++; if(h[l]>lm)lm=h[l]; res+=lm-h[l]; }
        else { r--; if(h[r]>rm)rm=h[r]; res+=rm-h[r]; }
    }
    printf("%d\n", res);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
using namespace std;
int main() {
    int n; cin >> n;
    if (n == 0) { cout << 0 << endl; return 0; }
    vector<int> h(n);
    for (int i = 0; i < n; i++) cin >> h[i];
    if (n < 3) { cout << 0 << endl; return 0; }
    int l=0, r=n-1, lm=h[0], rm=h[n-1], res=0;
    while (l < r) {
        if (lm <= rm) { l++; lm=max(lm,h[l]); res+=lm-h[l]; }
        else { r--; rm=max(rm,h[r]); res+=rm-h[r]; }
    }
    cout << res << endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    if (n == 0) { System.out.println(0); return; }
    int[] h = new int[n];
    for (int i = 0; i < n; i++) h[i] = sc.nextInt();
    if (n < 3) { System.out.println(0); return; }
    int l=0, r=n-1, lm=h[0], rm=h[n-1], res=0;
    while (l < r) {
        if (lm <= rm) { l++; lm=Math.max(lm,h[l]); res+=lm-h[l]; }
        else { r--; rm=Math.max(rm,h[r]); res+=rm-h[r]; }
    }
    System.out.println(res);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"histogram_largest_rectangle": {
"c": r"""
#include <stdio.h>
int main() {
    int n; scanf("%d", &n);
    if (n == 0) { printf("0\n"); return 0; }
    int h[100001];
    for (int i = 0; i < n; i++) scanf("%d", &h[i]);
    int st[100002], top=-1, mx=0;
    for (int i = 0; i <= n; i++) {
        int v = (i==n)?0:h[i];
        while (top>=0 && h[st[top]]>v) {
            int ht=h[st[top--]];
            int w = (top<0)?i:i-st[top]-1;
            if(ht*w>mx)mx=ht*w;
        }
        st[++top]=i;
    }
    printf("%d\n", mx);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <stack>
using namespace std;
int main() {
    int n; cin >> n;
    if (n == 0) { cout << 0 << endl; return 0; }
    vector<int> h(n);
    for (int i = 0; i < n; i++) cin >> h[i];
    stack<int> st; int mx=0;
    for (int i = 0; i <= n; i++) {
        int v = (i==n)?0:h[i];
        while (!st.empty() && h[st.top()]>v) {
            int ht=h[st.top()]; st.pop();
            int w = st.empty()?i:i-st.top()-1;
            mx=max(mx,ht*w);
        }
        st.push(i);
    }
    cout << mx << endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    if (n == 0) { System.out.println(0); return; }
    int[] h = new int[n];
    for (int i = 0; i < n; i++) h[i] = sc.nextInt();
    Deque<Integer> st = new ArrayDeque<>();
    int mx = 0;
    for (int i = 0; i <= n; i++) {
        int v = (i==n)?0:h[i];
        while (!st.isEmpty() && h[st.peek()]>v) {
            int ht = h[st.pop()];
            int w = st.isEmpty()?i:i-st.peek()-1;
            mx = Math.max(mx, ht*w);
        }
        st.push(i);
    }
    System.out.println(mx);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"decode_ways": {
"c": r"""
#include <stdio.h>
#include <string.h>
int main() {
    char s[105]; scanf("%s", s);
    int n = strlen(s);
    if (n==0 || s[0]=='0') { printf("0\n"); return 0; }
    long long dp[105]={0}; dp[0]=dp[1]=1;
    for (int i=2; i<=n; i++) {
        if (s[i-1]!='0') dp[i]+=dp[i-1];
        int two=(s[i-2]-'0')*10+(s[i-1]-'0');
        if (two>=10 && two<=26) dp[i]+=dp[i-2];
    }
    printf("%lld\n", dp[n]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
using namespace std;
int main() {
    string s; cin >> s;
    int n = s.size();
    if (n==0 || s[0]=='0') { cout << 0 << endl; return 0; }
    vector<long long> dp(n+1,0); dp[0]=dp[1]=1;
    for (int i=2; i<=n; i++) {
        if (s[i-1]!='0') dp[i]+=dp[i-1];
        int two=(s[i-2]-'0')*10+(s[i-1]-'0');
        if (two>=10 && two<=26) dp[i]+=dp[i-2];
    }
    cout << dp[n] << endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    String s = sc.next();
    int n = s.length();
    if (n==0 || s.charAt(0)=='0') { System.out.println(0); return; }
    long[] dp = new long[n+1]; dp[0]=dp[1]=1;
    for (int i=2; i<=n; i++) {
        if (s.charAt(i-1)!='0') dp[i]+=dp[i-1];
        int two=(s.charAt(i-2)-'0')*10+(s.charAt(i-1)-'0');
        if (two>=10 && two<=26) dp[i]+=dp[i-2];
    }
    System.out.println(dp[n]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"edit_distance": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXN 1005
int dp[MAXN];
int main() {
    char s[MAXN], t[MAXN];
    if (!fgets(s, MAXN, stdin)) s[0]='\0';
    if (!fgets(t, MAXN, stdin)) t[0]='\0';
    int sl=strlen(s); while(sl>0&&(s[sl-1]=='\n'||s[sl-1]=='\r'))s[--sl]='\0';
    int tl=strlen(t); while(tl>0&&(t[tl-1]=='\n'||t[tl-1]=='\r'))t[--tl]='\0';
    for (int j=0;j<=tl;j++) dp[j]=j;
    for (int i=1;i<=sl;i++) {
        int prev=dp[0]; dp[0]=i;
        for (int j=1;j<=tl;j++) {
            int tmp=dp[j];
            if (s[i-1]==t[j-1]) dp[j]=prev;
            else { int m=prev<dp[j]?prev:dp[j]; m=m<dp[j-1]?m:dp[j-1]; dp[j]=1+m; }
            prev=tmp;
        }
    }
    printf("%d\n", dp[tl]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
using namespace std;
int main() {
    string s, t;
    getline(cin, s); getline(cin, t);
    int sl=s.size(), tl=t.size();
    vector<int> dp(tl+1);
    for (int j=0;j<=tl;j++) dp[j]=j;
    for (int i=1;i<=sl;i++) {
        int prev=dp[0]; dp[0]=i;
        for (int j=1;j<=tl;j++) {
            int tmp=dp[j];
            dp[j]=(s[i-1]==t[j-1])?prev:1+min({prev,dp[j],dp[j-1]});
            prev=tmp;
        }
    }
    cout << dp[tl] << endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String s = br.readLine(); if(s==null)s="";
    String t = br.readLine(); if(t==null)t="";
    int sl=s.length(), tl=t.length();
    int[] dp = new int[tl+1];
    for (int j=0;j<=tl;j++) dp[j]=j;
    for (int i=1;i<=sl;i++) {
        int prev=dp[0]; dp[0]=i;
        for (int j=1;j<=tl;j++) {
            int tmp=dp[j];
            dp[j]=(s.charAt(i-1)==t.charAt(j-1))?prev:1+Math.min(prev,Math.min(dp[j],dp[j-1]));
            prev=tmp;
        }
    }
    System.out.println(dp[tl]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"distinct_subsequences": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXN 1005
long long dp[MAXN];
int main() {
    char s[MAXN], t[MAXN];
    if (!fgets(s, MAXN, stdin)) s[0]='\0';
    if (!fgets(t, MAXN, stdin)) t[0]='\0';
    int sl=strlen(s); while(sl>0&&(s[sl-1]=='\n'||s[sl-1]=='\r'))s[--sl]='\0';
    int tl=strlen(t); while(tl>0&&(t[tl-1]=='\n'||t[tl-1]=='\r'))t[--tl]='\0';
    for (int j=0;j<=tl;j++) dp[j]=0; dp[0]=1;
    for (int i=0;i<sl;i++)
        for (int j=tl;j>=1;j--)
            if (s[i]==t[j-1]) dp[j]+=dp[j-1];
    printf("%lld\n", dp[tl]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
using namespace std;
int main() {
    string s, t;
    getline(cin, s); getline(cin, t);
    int sl=s.size(), tl=t.size();
    vector<long long> dp(tl+1,0); dp[0]=1;
    for (char c: s)
        for (int j=tl;j>=1;j--)
            if (c==t[j-1]) dp[j]+=dp[j-1];
    cout << dp[tl] << endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String s = br.readLine(); if(s==null)s="";
    String t = br.readLine(); if(t==null)t="";
    int tl=t.length();
    long[] dp = new long[tl+1]; dp[0]=1;
    for (char c: s.toCharArray())
        for (int j=tl;j>=1;j--)
            if (c==t.charAt(j-1)) dp[j]+=dp[j-1];
    System.out.println(dp[tl]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"palindrome_min_cuts": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXN 1005
int ip[MAXN][MAXN], cuts[MAXN];
int main() {
    char s[MAXN]; scanf("%s", s);
    int n=strlen(s);
    if (n==0) { printf("0\n"); return 0; }
    for (int i=0;i<n;i++) for (int j=0;j<n;j++) ip[i][j]=0;
    for (int i=0;i<n;i++) ip[i][i]=1;
    for (int i=0;i<n-1;i++) ip[i][i+1]=(s[i]==s[i+1]);
    for (int l=3;l<=n;l++)
        for (int i=0;i<=n-l;i++) { int j=i+l-1; ip[i][j]=ip[i+1][j-1]&&s[i]==s[j]; }
    for (int i=0;i<=n;i++) cuts[i]=i-1;
    for (int i=0;i<n;i++) {
        if (ip[0][i]) { cuts[i+1]=0; continue; }
        for (int j=0;j<i;j++)
            if (ip[j+1][i] && cuts[j+1]+1<cuts[i+1]) cuts[i+1]=cuts[j+1]+1;
    }
    printf("%d\n", cuts[n]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
#include <climits>
using namespace std;
int main() {
    string s; cin >> s;
    int n=s.size();
    if (n==0) { cout << 0 << endl; return 0; }
    vector<vector<bool>> ip(n,vector<bool>(n,false));
    for (int i=0;i<n;i++) ip[i][i]=true;
    for (int i=0;i<n-1;i++) ip[i][i+1]=(s[i]==s[i+1]);
    for (int l=3;l<=n;l++)
        for (int i=0;i<=n-l;i++) { int j=i+l-1; ip[i][j]=ip[i+1][j-1]&&s[i]==s[j]; }
    vector<int> cuts(n+1);
    for (int i=0;i<=n;i++) cuts[i]=i-1;
    for (int i=0;i<n;i++) {
        if (ip[0][i]) { cuts[i+1]=0; continue; }
        for (int j=0;j<i;j++)
            if (ip[j+1][i]) cuts[i+1]=min(cuts[i+1],cuts[j+1]+1);
    }
    cout << cuts[n] << endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    String s = sc.next();
    int n = s.length();
    if (n==0) { System.out.println(0); return; }
    boolean[][] ip = new boolean[n][n];
    for (int i=0;i<n;i++) ip[i][i]=true;
    for (int i=0;i<n-1;i++) ip[i][i+1]=(s.charAt(i)==s.charAt(i+1));
    for (int l=3;l<=n;l++)
        for (int i=0;i<=n-l;i++) { int j=i+l-1; ip[i][j]=ip[i+1][j-1]&&s.charAt(i)==s.charAt(j); }
    int[] cuts = new int[n+1];
    for (int i=0;i<=n;i++) cuts[i]=i-1;
    for (int i=0;i<n;i++) {
        if (ip[0][i]) { cuts[i+1]=0; continue; }
        for (int j=0;j<i;j++)
            if (ip[j+1][i]) cuts[i+1]=Math.min(cuts[i+1],cuts[j+1]+1);
    }
    System.out.println(cuts[n]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"burst_balloons": {
"c": r"""
#include <stdio.h>
int a[305], dp[305][305];
int max2(int x,int y){return x>y?x:y;}
int main() {
    int n; scanf("%d",&n);
    int nums[305];
    for (int i=0;i<n;i++) scanf("%d",&nums[i]);
    a[0]=1; for(int i=0;i<n;i++) a[i+1]=nums[i]; a[n+1]=1;
    int sz=n+2;
    for(int i=0;i<sz;i++) for(int j=0;j<sz;j++) dp[i][j]=0;
    for(int l=2;l<sz;l++)
        for(int i=0;i<sz-l;i++) {
            int j=i+l;
            for(int k=i+1;k<j;k++)
                dp[i][j]=max2(dp[i][j],a[i]*a[k]*a[j]+dp[i][k]+dp[k][j]);
        }
    printf("%d\n",dp[0][sz-1]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;
int main() {
    int n; cin>>n;
    vector<int> nums(n);
    for(int i=0;i<n;i++) cin>>nums[i];
    vector<int> a; a.push_back(1);
    for(int x:nums) a.push_back(x); a.push_back(1);
    int sz=a.size();
    vector<vector<int>> dp(sz,vector<int>(sz,0));
    for(int l=2;l<sz;l++)
        for(int i=0;i<sz-l;i++) {
            int j=i+l;
            for(int k=i+1;k<j;k++)
                dp[i][j]=max(dp[i][j],a[i]*a[k]*a[j]+dp[i][k]+dp[k][j]);
        }
    cout<<dp[0][sz-1]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    int[] nums = new int[n];
    for(int i=0;i<n;i++) nums[i]=sc.nextInt();
    int sz=n+2; int[] a=new int[sz];
    a[0]=1; for(int i=0;i<n;i++) a[i+1]=nums[i]; a[n+1]=1;
    int[][] dp=new int[sz][sz];
    for(int l=2;l<sz;l++)
        for(int i=0;i<sz-l;i++) {
            int j=i+l;
            for(int k=i+1;k<j;k++)
                dp[i][j]=Math.max(dp[i][j],a[i]*a[k]*a[j]+dp[i][k]+dp[k][j]);
        }
    System.out.println(dp[0][sz-1]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"word_break": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXW 200
#define MAXD 500
char w[MAXW], dict[MAXD][MAXW];
int dp[MAXW];
int main() {
    if (!fgets(w, MAXW, stdin)) w[0]='\0';
    int wl=strlen(w); while(wl>0&&(w[wl-1]=='\n'||w[wl-1]=='\r'))w[--wl]='\0';
    int nd; scanf("%d\n",&nd);
    for(int i=0;i<nd;i++){
        if(!fgets(dict[i],MAXW,stdin)) dict[i][0]='\0';
        int dl=strlen(dict[i]); while(dl>0&&(dict[i][dl-1]=='\n'||dict[i][dl-1]=='\r'))dict[i][--dl]='\0';
    }
    for(int i=0;i<=wl;i++) dp[i]=0; dp[0]=1;
    for(int i=1;i<=wl;i++)
        for(int j=0;j<i&&!dp[i];j++)
            if(dp[j])
                for(int k=0;k<nd;k++)
                    if(strncmp(w+j,dict[k],i-j)==0 && dict[k][i-j]=='\0'){dp[i]=1;break;}
    printf("%s\n",dp[wl]?"True":"False");
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
using namespace std;
int main() {
    string w; getline(cin,w);
    int nd; cin>>nd; cin.ignore();
    unordered_set<string> d;
    for(int i=0;i<nd;i++){ string x; getline(cin,x); d.insert(x); }
    int wl=w.size();
    vector<bool> dp(wl+1,false); dp[0]=true;
    for(int i=1;i<=wl;i++)
        for(int j=0;j<i&&!dp[i];j++)
            if(dp[j]&&d.count(w.substr(j,i-j))) dp[i]=true;
    cout<<(dp[wl]?"True":"False")<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String w = br.readLine(); if(w==null)w="";
    int nd = Integer.parseInt(br.readLine().trim());
    Set<String> d = new HashSet<>();
    for(int i=0;i<nd;i++){ String x=br.readLine(); if(x!=null) d.add(x); }
    int wl=w.length();
    boolean[] dp=new boolean[wl+1]; dp[0]=true;
    for(int i=1;i<=wl;i++)
        for(int j=0;j<i&&!dp[i];j++)
            if(dp[j]&&d.contains(w.substring(j,i))) dp[i]=true;
    System.out.println(dp[wl]?"True":"False");
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"min_jumps": {
"c": r"""
#include <stdio.h>
int main() {
    int n; scanf("%d",&n);
    int nums[100001];
    for(int i=0;i<n;i++) scanf("%d",&nums[i]);
    if(n<=1){printf("0\n");return 0;}
    int j=0,ce=0,far=0;
    for(int i=0;i<n-1;i++){
        if(i+nums[i]>far) far=i+nums[i];
        if(i==ce){j++;ce=far;}
        if(ce>=n-1) break;
    }
    printf("%d\n",j);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
using namespace std;
int main() {
    int n; cin>>n;
    vector<int> nums(n);
    for(int i=0;i<n;i++) cin>>nums[i];
    if(n<=1){cout<<0<<endl;return 0;}
    int j=0,ce=0,far=0;
    for(int i=0;i<n-1;i++){
        far=max(far,i+nums[i]);
        if(i==ce){j++;ce=far;}
        if(ce>=n-1) break;
    }
    cout<<j<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    int[] nums = new int[n];
    for(int i=0;i<n;i++) nums[i]=sc.nextInt();
    if(n<=1){System.out.println(0);return;}
    int j=0,ce=0,far=0;
    for(int i=0;i<n-1;i++){
        far=Math.max(far,i+nums[i]);
        if(i==ce){j++;ce=far;}
        if(ce>=n-1) break;
    }
    System.out.println(j);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"first_missing_positive": {
"c": r"""
#include <stdio.h>
int main() {
    int n; scanf("%d",&n);
    if(n==0){printf("1\n");return 0;}
    int a[100001];
    for(int i=0;i<n;i++) scanf("%d",&a[i]);
    for(int i=0;i<n;i++){
        while(a[i]>=1&&a[i]<=n&&a[a[i]-1]!=a[i]){int t=a[a[i]-1];a[a[i]-1]=a[i];a[i]=t;}
    }
    for(int i=0;i<n;i++) if(a[i]!=i+1){printf("%d\n",i+1);return 0;}
    printf("%d\n",n+1);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
using namespace std;
int main() {
    int n; cin>>n;
    if(n==0){cout<<1<<endl;return 0;}
    vector<int> a(n);
    for(int i=0;i<n;i++) cin>>a[i];
    for(int i=0;i<n;i++)
        while(a[i]>=1&&a[i]<=n&&a[a[i]-1]!=a[i]) swap(a[i],a[a[i]-1]);
    for(int i=0;i<n;i++) if(a[i]!=i+1){cout<<i+1<<endl;return 0;}
    cout<<n+1<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    if(n==0){System.out.println(1);return;}
    int[] a = new int[n];
    for(int i=0;i<n;i++) a[i]=sc.nextInt();
    for(int i=0;i<n;i++)
        while(a[i]>=1&&a[i]<=n&&a[a[i]-1]!=a[i]){int t=a[a[i]-1];a[a[i]-1]=a[i];a[i]=t;}
    for(int i=0;i<n;i++) if(a[i]!=i+1){System.out.println(i+1);return;}
    System.out.println(n+1);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"longest_consecutive_sequence": {
"c": r"""
#include <stdio.h>
#include <stdlib.h>
int cmp(const void*a,const void*b){return *(int*)a-*(int*)b;}
int main() {
    int n; scanf("%d",&n);
    if(n==0){printf("0\n");return 0;}
    int a[100001];
    for(int i=0;i<n;i++) scanf("%d",&a[i]);
    qsort(a,n,sizeof(int),cmp);
    int best=1,cur=1;
    for(int i=1;i<n;i++){
        if(a[i]==a[i-1]+1){cur++;if(cur>best)best=cur;}
        else if(a[i]!=a[i-1]) cur=1;
    }
    printf("%d\n",best);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <unordered_set>
using namespace std;
int main() {
    int n; cin>>n;
    if(n==0){cout<<0<<endl;return 0;}
    vector<int> nums(n);
    for(int i=0;i<n;i++) cin>>nums[i];
    unordered_set<int> s(nums.begin(),nums.end());
    int best=0;
    for(int v:s){
        if(!s.count(v-1)){
            int cur=v,st=1;
            while(s.count(cur+1)){cur++;st++;}
            best=max(best,st);
        }
    }
    cout<<best<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    if(n==0){System.out.println(0);return;}
    Set<Integer> s = new HashSet<>();
    for(int i=0;i<n;i++) s.add(sc.nextInt());
    int best=0;
    for(int v:s){
        if(!s.contains(v-1)){
            int cur=v,st=1;
            while(s.contains(cur+1)){cur++;st++;}
            best=Math.max(best,st);
        }
    }
    System.out.println(best);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"sliding_window_maximum": {
"c": r"""
#include <stdio.h>
int dq[100002], nums[100002];
int main() {
    int n; scanf("%d",&n);
    for(int i=0;i<n;i++) scanf("%d",&nums[i]);
    int k; scanf("%d",&k);
    int head=0,tail=0; int first=1;
    for(int i=0;i<n;i++){
        while(head<tail&&dq[head]<i-k+1) head++;
        while(head<tail&&nums[dq[tail-1]]<nums[i]) tail--;
        dq[tail++]=i;
        if(i>=k-1){
            if(!first) printf(" ");
            printf("%d",nums[dq[head]]);
            first=0;
        }
    }
    printf("\n");
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <deque>
using namespace std;
int main() {
    int n; cin>>n;
    vector<int> nums(n);
    for(int i=0;i<n;i++) cin>>nums[i];
    int k; cin>>k;
    deque<int> dq; vector<int> res;
    for(int i=0;i<n;i++){
        while(!dq.empty()&&dq.front()<i-k+1) dq.pop_front();
        while(!dq.empty()&&nums[dq.back()]<nums[i]) dq.pop_back();
        dq.push_back(i);
        if(i>=k-1) res.push_back(nums[dq.front()]);
    }
    for(int i=0;i<(int)res.size();i++){if(i)cout<<" ";cout<<res[i];}
    cout<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int n = sc.nextInt();
    int[] nums = new int[n];
    for(int i=0;i<n;i++) nums[i]=sc.nextInt();
    int k = sc.nextInt();
    Deque<Integer> dq = new ArrayDeque<>();
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<n;i++){
        while(!dq.isEmpty()&&dq.peekFirst()<i-k+1) dq.pollFirst();
        while(!dq.isEmpty()&&nums[dq.peekLast()]<nums[i]) dq.pollLast();
        dq.addLast(i);
        if(i>=k-1){if(sb.length()>0)sb.append(' ');sb.append(nums[dq.peekFirst()]);}
    }
    System.out.println(sb);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"n_queens_count": {
"c": r"""
#include <stdio.h>
int N;
int bt(int r,int c,int d1,int d2){
    if(r==N) return 1;
    int cnt=0,avail=((1<<N)-1)&~(c|d1|d2);
    while(avail){int bit=avail&(-avail);avail-=bit;cnt+=bt(r+1,c|bit,(d1|bit)<<1,(d2|bit)>>1);}
    return cnt;
}
int main(){scanf("%d",&N);printf("%d\n",bt(0,0,0,0));return 0;}
""",
"cpp": r"""
#include <iostream>
using namespace std;
int N;
int bt(int r,int c,int d1,int d2){
    if(r==N) return 1;
    int cnt=0,avail=((1<<N)-1)&~(c|d1|d2);
    while(avail){int bit=avail&(-avail);avail-=bit;cnt+=bt(r+1,c|bit,(d1|bit)<<1,(d2|bit)>>1);}
    return cnt;
}
int main(){cin>>N;cout<<bt(0,0,0,0)<<endl;return 0;}
""",
"java": r"""
static int N;
static int bt(int r,int c,int d1,int d2){
    if(r==N) return 1;
    int cnt=0,avail=((1<<N)-1)&~(c|d1|d2);
    while(avail!=0){int bit=avail&(-avail);avail-=bit;cnt+=bt(r+1,c|bit,(d1|bit)<<1,(d2|bit)>>>1);}
    return cnt;
}
public static void main(String[] args){
    Scanner sc=new Scanner(System.in); N=sc.nextInt();
    System.out.println(bt(0,0,0,0));
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"count_inversions": {
"c": r"""
#include <stdio.h>
long long cnt=0;
void ms(int*a,int*tmp,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms(a,tmp,l,m); ms(a,tmp,m+1,r);
    int i=l,j=m+1,k=l;
    while(i<=m&&j<=r){
        if(a[i]<=a[j]) tmp[k++]=a[i++];
        else{cnt+=m-i+1;tmp[k++]=a[j++];}
    }
    while(i<=m) tmp[k++]=a[i++];
    while(j<=r) tmp[k++]=a[j++];
    for(int x=l;x<=r;x++) a[x]=tmp[x];
}
int main(){
    int n; scanf("%d",&n);
    int a[100001],tmp[100001];
    for(int i=0;i<n;i++) scanf("%d",&a[i]);
    ms(a,tmp,0,n-1);
    printf("%lld\n",cnt);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
using namespace std;
long long cnt=0;
void ms(vector<int>&a,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms(a,l,m); ms(a,m+1,r);
    vector<int> tmp;
    int i=l,j=m+1;
    while(i<=m&&j<=r){
        if(a[i]<=a[j]) tmp.push_back(a[i++]);
        else{cnt+=m-i+1;tmp.push_back(a[j++]);}
    }
    while(i<=m) tmp.push_back(a[i++]);
    while(j<=r) tmp.push_back(a[j++]);
    for(int k=l;k<=r;k++) a[k]=tmp[k-l];
}
int main(){
    int n; cin>>n;
    vector<int> a(n);
    for(int i=0;i<n;i++) cin>>a[i];
    ms(a,0,n-1);
    cout<<cnt<<endl;
    return 0;
}
""",
"java": r"""
static long cnt=0;
static void ms(int[]a,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms(a,l,m); ms(a,m+1,r);
    int[]tmp=new int[r-l+1];
    int i=l,j=m+1,k=0;
    while(i<=m&&j<=r){
        if(a[i]<=a[j]) tmp[k++]=a[i++];
        else{cnt+=m-i+1;tmp[k++]=a[j++];}
    }
    while(i<=m) tmp[k++]=a[i++];
    while(j<=r) tmp[k++]=a[j++];
    for(int x=0;x<tmp.length;x++) a[l+x]=tmp[x];
}
public static void main(String[] args){
    cnt=0;
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt();
    int[]a=new int[n];
    for(int i=0;i<n;i++) a[i]=sc.nextInt();
    ms(a,0,n-1);
    System.out.println(cnt);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"matrix_chain_multiplication": {
"c": r"""
#include <stdio.h>
int dp[105][105];
int main(){
    int n; scanf("%d",&n);
    int d[106];
    for(int i=0;i<=n;i++) scanf("%d",&d[i]);
    for(int i=0;i<n;i++) for(int j=0;j<n;j++) dp[i][j]=0;
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1; dp[i][j]=2000000000;
            for(int k=i;k<j;k++){
                int v=dp[i][k]+dp[k+1][j]+d[i]*d[k+1]*d[j+1];
                if(v<dp[i][j]) dp[i][j]=v;
            }
        }
    printf("%d\n",dp[0][n-1]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <climits>
using namespace std;
int main(){
    int n; cin>>n;
    vector<int> d(n+1);
    for(int i=0;i<=n;i++) cin>>d[i];
    vector<vector<int>> dp(n,vector<int>(n,0));
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1; dp[i][j]=INT_MAX;
            for(int k=i;k<j;k++)
                dp[i][j]=min(dp[i][j],dp[i][k]+dp[k+1][j]+d[i]*d[k+1]*d[j+1]);
        }
    cout<<dp[0][n-1]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt();
    int[]d=new int[n+1];
    for(int i=0;i<=n;i++) d[i]=sc.nextInt();
    int[][]dp=new int[n][n];
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1; dp[i][j]=Integer.MAX_VALUE;
            for(int k=i;k<j;k++){
                int v=dp[i][k]+dp[k+1][j]+d[i]*d[k+1]*d[j+1];
                if(v<dp[i][j]) dp[i][j]=v;
            }
        }
    System.out.println(dp[0][n-1]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"egg_drop_minimum_trials": {
"c": r"""
#include <stdio.h>
int main(){
    int k,n; scanf("%d %d",&k,&n);
    if(k==1){printf("%d\n",n);return 0;}
    if(n<=1){printf("%d\n",n);return 0;}
    /* dp[m] = max floors testable with m trials and k eggs */
    int dp[10001]={0};
    int m=0;
    while(dp[m]<n){
        m++;
        /* new dp[m] computed from old dp values */
        for(int i=k;i>=1;i--)
            dp[m]=dp[m-1]+(i>1?dp[m-1]:0)+1; /* wrong - need per-egg */
        /* Proper: dp_new[e] = dp_old[e-1] + dp_old[e] + 1 for each egg count */
        /* Reset and redo properly */
    }
    /* Redo with 2D properly */
    int eggs[105][10001];
    for(int i=0;i<=k;i++) for(int j=0;j<=n;j++) eggs[i][j]=0;
    m=0;
    int done=0;
    while(!done){
        m++;
        for(int e=1;e<=k;e++){
            eggs[e][m]=eggs[e-1][m-1]+eggs[e][m-1]+1;
            if(eggs[e][m]>1000000) eggs[e][m]=1000001;
        }
        if(eggs[k][m]>=n) done=1;
    }
    printf("%d\n",m);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
using namespace std;
int main(){
    int k,n; cin>>k>>n;
    if(k==1){cout<<n<<endl;return 0;}
    if(n<=1){cout<<n<<endl;return 0;}
    /* dp[m][e] = max floors testable with m moves and e eggs */
    vector<vector<long long>> dp(n+2,vector<long long>(k+1,0));
    for(int m=1;m<=n;m++){
        for(int e=1;e<=k;e++)
            dp[m][e]=dp[m-1][e-1]+dp[m-1][e]+1;
        if(dp[m][k]>=n){cout<<m<<endl;return 0;}
    }
    cout<<n<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int k=sc.nextInt(),n=sc.nextInt();
    if(k==1){System.out.println(n);return;}
    if(n<=1){System.out.println(n);return;}
    long[][]dp=new long[n+2][k+1];
    for(int m=1;m<=n;m++){
        for(int e=1;e<=k;e++)
            dp[m][e]=dp[m-1][e-1]+dp[m-1][e]+1;
        if(dp[m][k]>=n){System.out.println(m);return;}
    }
    System.out.println(n);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"painters_partition": {
"c": r"""
#include <stdio.h>
int n; long long planks[100001];
int ok(long long mid,int k){
    int p=1; long long c=0;
    for(int i=0;i<n;i++){
        if(planks[i]>mid) return 0;
        if(c+planks[i]>mid){p++;c=0;}
        c+=planks[i];
    }
    return p<=k;
}
int main(){
    int k; scanf("%d",&k); scanf("%d",&n);
    for(int i=0;i<n;i++) scanf("%lld",&planks[i]);
    long long lo=0,hi=0;
    for(int i=0;i<n;i++){if(planks[i]>lo)lo=planks[i];hi+=planks[i];}
    while(lo<hi){long long mid=lo+(hi-lo)/2;if(ok(mid,k))hi=mid;else lo=mid+1;}
    printf("%lld\n",lo);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <numeric>
#include <algorithm>
using namespace std;
int main(){
    int k,n; cin>>k>>n;
    vector<long long> p(n);
    for(int i=0;i<n;i++) cin>>p[i];
    auto ok=[&](long long mid)->bool{
        int painters=1; long long c=0;
        for(auto x:p){if(x>mid)return false;if(c+x>mid){painters++;c=0;}c+=x;}
        return painters<=k;
    };
    long long lo=*max_element(p.begin(),p.end()),hi=accumulate(p.begin(),p.end(),0LL);
    while(lo<hi){long long mid=lo+(hi-lo)/2;if(ok(mid))hi=mid;else lo=mid+1;}
    cout<<lo<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int k=sc.nextInt(),n=sc.nextInt();
    long[]p=new long[n]; long lo=0,hi=0;
    for(int i=0;i<n;i++){p[i]=sc.nextLong();if(p[i]>lo)lo=p[i];hi+=p[i];}
    while(lo<hi){
        long mid=lo+(hi-lo)/2;
        int painters=1; long c=0; boolean bad=false;
        for(long x:p){if(x>mid){bad=true;break;}if(c+x>mid){painters++;c=0;}c+=x;}
        if(!bad&&painters<=k) hi=mid; else lo=mid+1;
    }
    System.out.println(lo);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"longest_palindromic_substring_length": {
"c": r"""
#include <stdio.h>
#include <string.h>
int main(){
    char s[100002]; scanf("%s",s);
    int sn=strlen(s);
    if(sn==0){printf("0\n");return 0;}
    /* Manacher's */
    int tn=2*sn+1;
    char t[200005]; int p[200005];
    t[0]='#';
    for(int i=0;i<sn;i++){t[2*i+1]=s[i];t[2*i+2]='#';}
    t[tn]='\0';
    int c=0,r=0,mx=0;
    for(int i=0;i<tn;i++){
        p[i]=(i<r)?((p[2*c-i]<r-i)?p[2*c-i]:r-i):0;
        while(i-p[i]-1>=0&&i+p[i]+1<tn&&t[i-p[i]-1]==t[i+p[i]+1]) p[i]++;
        if(i+p[i]>r){c=i;r=i+p[i];}
        if(p[i]>mx) mx=p[i];
    }
    printf("%d\n",mx);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
using namespace std;
int main(){
    string s; cin>>s;
    if(s.empty()){cout<<0<<endl;return 0;}
    string t="#";
    for(char c:s){t+=c;t+='#';}
    int tn=t.size();
    vector<int> p(tn,0);
    int c=0,r=0,mx=0;
    for(int i=0;i<tn;i++){
        p[i]=(i<r)?min(p[2*c-i],r-i):0;
        while(i-p[i]-1>=0&&i+p[i]+1<tn&&t[i-p[i]-1]==t[i+p[i]+1]) p[i]++;
        if(i+p[i]>r){c=i;r=i+p[i];}
        mx=max(mx,p[i]);
    }
    cout<<mx<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    String s=sc.next();
    if(s.isEmpty()){System.out.println(0);return;}
    StringBuilder tb=new StringBuilder("#");
    for(char c:s.toCharArray()){tb.append(c);tb.append('#');}
    String t=tb.toString(); int tn=t.length();
    int[]p=new int[tn]; int c2=0,r=0,mx=0;
    for(int i=0;i<tn;i++){
        p[i]=(i<r)?Math.min(p[2*c2-i],r-i):0;
        while(i-p[i]-1>=0&&i+p[i]+1<tn&&t.charAt(i-p[i]-1)==t.charAt(i+p[i]+1)) p[i]++;
        if(i+p[i]>r){c2=i;r=i+p[i];}
        mx=Math.max(mx,p[i]);
    }
    System.out.println(mx);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"k_transactions_max_profit": {
"c": r"""
#include <stdio.h>
#include <string.h>
int main(){
    int k,n; scanf("%d",&k); scanf("%d",&n);
    if(n==0||k==0){printf("0\n");return 0;}
    int prices[100001];
    for(int i=0;i<n;i++) scanf("%d",&prices[i]);
    if(k>=n/2){
        int res=0;
        for(int i=1;i<n;i++) if(prices[i]>prices[i-1]) res+=prices[i]-prices[i-1];
        printf("%d\n",res); return 0;
    }
    int buy[1001],sell[1001];
    for(int i=0;i<=k;i++){buy[i]=-1000000;sell[i]=0;}
    for(int i=0;i<n;i++)
        for(int j=k;j>=1;j--){
            if(sell[j-1]-prices[i]>buy[j]) buy[j]=sell[j-1]-prices[i];
            if(buy[j]+prices[i]>sell[j]) sell[j]=buy[j]+prices[i];
        }
    printf("%d\n",sell[k]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;
int main(){
    int k,n; cin>>k>>n;
    if(n==0||k==0){cout<<0<<endl;return 0;}
    vector<int> p(n);
    for(int i=0;i<n;i++) cin>>p[i];
    if(k>=n/2){
        int res=0;
        for(int i=1;i<n;i++) if(p[i]>p[i-1]) res+=p[i]-p[i-1];
        cout<<res<<endl; return 0;
    }
    vector<int> buy(k+1,-1e9),sell(k+1,0);
    for(int i=0;i<n;i++)
        for(int j=k;j>=1;j--){
            buy[j]=max(buy[j],sell[j-1]-p[i]);
            sell[j]=max(sell[j],buy[j]+p[i]);
        }
    cout<<sell[k]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int k=sc.nextInt(),n=sc.nextInt();
    if(n==0||k==0){System.out.println(0);return;}
    int[]p=new int[n];
    for(int i=0;i<n;i++) p[i]=sc.nextInt();
    if(k>=n/2){
        int res=0;
        for(int i=1;i<n;i++) if(p[i]>p[i-1]) res+=p[i]-p[i-1];
        System.out.println(res); return;
    }
    int[]buy=new int[k+1],sell=new int[k+1];
    java.util.Arrays.fill(buy,Integer.MIN_VALUE/2);
    for(int i=0;i<n;i++)
        for(int j=k;j>=1;j--){
            buy[j]=Math.max(buy[j],sell[j-1]-p[i]);
            sell[j]=Math.max(sell[j],buy[j]+p[i]);
        }
    System.out.println(sell[k]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"maximum_sum_submatrix": {
"c": r"""
#include <stdio.h>
#include <limits.h>
int main(){
    int rows,cols; scanf("%d %d",&rows,&cols);
    int mat[105][105];
    for(int i=0;i<rows;i++) for(int j=0;j<cols;j++) scanf("%d",&mat[i][j]);
    int res=INT_MIN;
    for(int l=0;l<cols;l++){
        int tmp[105]={0};
        for(int r=l;r<cols;r++){
            for(int i=0;i<rows;i++) tmp[i]+=mat[i][r];
            int cur=tmp[0],mx=tmp[0];
            for(int i=1;i<rows;i++){cur=tmp[i]+(cur>0?cur:0);if(cur>mx)mx=cur;}
            if(mx>res) res=mx;
        }
    }
    printf("%d\n",res);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <climits>
#include <algorithm>
using namespace std;
int main(){
    int rows,cols; cin>>rows>>cols;
    vector<vector<int>> mat(rows,vector<int>(cols));
    for(int i=0;i<rows;i++) for(int j=0;j<cols;j++) cin>>mat[i][j];
    int res=INT_MIN;
    for(int l=0;l<cols;l++){
        vector<int> tmp(rows,0);
        for(int r=l;r<cols;r++){
            for(int i=0;i<rows;i++) tmp[i]+=mat[i][r];
            int cur=tmp[0],mx=tmp[0];
            for(int i=1;i<rows;i++){cur=tmp[i]+max(cur,0);mx=max(mx,cur);}
            res=max(res,mx);
        }
    }
    cout<<res<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int rows=sc.nextInt(),cols=sc.nextInt();
    int[][]mat=new int[rows][cols];
    for(int i=0;i<rows;i++) for(int j=0;j<cols;j++) mat[i][j]=sc.nextInt();
    int res=Integer.MIN_VALUE;
    for(int l=0;l<cols;l++){
        int[]tmp=new int[rows];
        for(int r=l;r<cols;r++){
            for(int i=0;i<rows;i++) tmp[i]+=mat[i][r];
            int cur=tmp[0],mx=tmp[0];
            for(int i=1;i<rows;i++){cur=tmp[i]+Math.max(cur,0);mx=Math.max(mx,cur);}
            res=Math.max(res,mx);
        }
    }
    System.out.println(res);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"word_ladder_length": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXW 100
#define MAXN 1000
char beg[MAXW],end2[MAXW],wl[MAXN][MAXW];
int vis[MAXN],q[MAXN*2],dist[MAXN*2];
int main(){
    scanf("%s%s",beg,end2);
    int n; scanf("%d",&n);
    for(int i=0;i<n;i++) scanf("%s",wl[i]);
    int wlen=strlen(beg);
    /* BFS */
    int head=0,tail=0;
    /* find beg in wl */
    for(int i=0;i<n;i++){
        if(strcmp(wl[i],beg)==0){vis[i]=1;q[tail]=i;dist[tail]=1;tail++;}
    }
    int ans=0;
    while(head<tail){
        int ci=q[head],cd=dist[head]; head++;
        if(strcmp(wl[ci],end2)==0){ans=cd;break;}
        for(int i=0;i<n;i++){
            if(vis[i]) continue;
            int diff=0;
            for(int k=0;k<wlen;k++) diff+=(wl[ci][k]!=wl[i][k]);
            if(diff==1){vis[i]=1;q[tail]=i;dist[tail]=cd+1;tail++;}
        }
    }
    printf("%d\n",ans);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <unordered_set>
using namespace std;
int main(){
    string beg,end2; cin>>beg>>end2;
    int n; cin>>n;
    unordered_set<string> wl;
    for(int i=0;i<n;i++){string x;cin>>x;wl.insert(x);}
    if(!wl.count(end2)){cout<<0<<endl;return 0;}
    queue<pair<string,int>> q;
    q.push({beg,1}); wl.erase(beg);
    while(!q.empty()){
        auto[w,d]=q.front();q.pop();
        if(w==end2){cout<<d<<endl;return 0;}
        string t=w;
        for(int i=0;i<(int)t.size();i++){
            char orig=t[i];
            for(char c='a';c<='z';c++){
                t[i]=c;
                if(wl.count(t)){wl.erase(t);q.push({t,d+1});}
            }
            t[i]=orig;
        }
    }
    cout<<0<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    String beg=sc.next(),end2=sc.next();
    int n=sc.nextInt();
    Set<String> wl=new HashSet<>();
    for(int i=0;i<n;i++) wl.add(sc.next());
    if(!wl.contains(end2)){System.out.println(0);return;}
    Queue<String> q=new LinkedList<>();
    Map<String,Integer> dist=new HashMap<>();
    q.add(beg); dist.put(beg,1); wl.remove(beg);
    while(!q.isEmpty()){
        String w=q.poll(); int d=dist.get(w);
        if(w.equals(end2)){System.out.println(d);return;}
        char[]cs=w.toCharArray();
        for(int i=0;i<cs.length;i++){
            char orig=cs[i];
            for(char c='a';c<='z';c++){
                cs[i]=c; String t=new String(cs);
                if(wl.contains(t)){wl.remove(t);dist.put(t,d+1);q.add(t);}
            }
            cs[i]=orig;
        }
    }
    System.out.println(0);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"optimal_bst_minimum_cost": {
"c": r"""
#include <stdio.h>
int pre[105],dp[105][105];
int main(){
    int n; scanf("%d",&n);
    int freq[105];
    for(int i=0;i<n;i++) scanf("%d",&freq[i]);
    pre[0]=0;
    for(int i=0;i<n;i++) pre[i+1]=pre[i]+freq[i];
    for(int i=0;i<n;i++) for(int j=0;j<n;j++) dp[i][j]=(i==j)?freq[i]:0;
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1; int s=pre[j+1]-pre[i];
            dp[i][j]=2000000000;
            for(int r=i;r<=j;r++){
                int v=(r>i?dp[i][r-1]:0)+(r<j?dp[r+1][j]:0)+s;
                if(v<dp[i][j]) dp[i][j]=v;
            }
        }
    printf("%d\n",dp[0][n-1]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <climits>
using namespace std;
int main(){
    int n; cin>>n;
    vector<int> freq(n);
    for(int i=0;i<n;i++) cin>>freq[i];
    vector<int> pre(n+1,0);
    for(int i=0;i<n;i++) pre[i+1]=pre[i]+freq[i];
    vector<vector<int>> dp(n,vector<int>(n,0));
    for(int i=0;i<n;i++) dp[i][i]=freq[i];
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1,s=pre[j+1]-pre[i]; dp[i][j]=INT_MAX;
            for(int r=i;r<=j;r++){
                int v=(r>i?dp[i][r-1]:0)+(r<j?dp[r+1][j]:0)+s;
                dp[i][j]=min(dp[i][j],v);
            }
        }
    cout<<dp[0][n-1]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt();
    int[]freq=new int[n];
    for(int i=0;i<n;i++) freq[i]=sc.nextInt();
    int[]pre=new int[n+1];
    for(int i=0;i<n;i++) pre[i+1]=pre[i]+freq[i];
    int[][]dp=new int[n][n];
    for(int i=0;i<n;i++) dp[i][i]=freq[i];
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1,s=pre[j+1]-pre[i]; dp[i][j]=Integer.MAX_VALUE;
            for(int r=i;r<=j;r++){
                int v=(r>i?dp[i][r-1]:0)+(r<j?dp[r+1][j]:0)+s;
                if(v<dp[i][j]) dp[i][j]=v;
            }
        }
    System.out.println(dp[0][n-1]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"boolean_parenthesization": {
"c": r"""
#include <stdio.h>
#include <string.h>
long long T[105][105],F[105][105];
int main(){
    char expr[205]; scanf("%s",expr);
    int n=(strlen(expr)+1)/2;
    char sym[105],ops[105];
    for(int i=0;i<n;i++) sym[i]=expr[2*i];
    for(int i=0;i<n-1;i++) ops[i]=expr[2*i+1];
    for(int i=0;i<n;i++) for(int j=0;j<n;j++){T[i][j]=0;F[i][j]=0;}
    for(int i=0;i<n;i++){T[i][i]=(sym[i]=='T');F[i][i]=(sym[i]=='F');}
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1;
            for(int k=i;k<j;k++){
                long long lt=T[i][k],lf=F[i][k],rt=T[k+1][j],rf=F[k+1][j];
                if(ops[k]=='|'){T[i][j]+=lt*rt+lt*rf+lf*rt;F[i][j]+=lf*rf;}
                else if(ops[k]=='&'){T[i][j]+=lt*rt;F[i][j]+=lf*rf+lf*rt+lt*rf;}
                else{T[i][j]+=lt*rf+lf*rt;F[i][j]+=lt*rt+lf*rf;}
            }
        }
    printf("%lld\n",T[0][n-1]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
using namespace std;
int main(){
    string expr; cin>>expr;
    int n=(expr.size()+1)/2;
    string sym="",ops="";
    for(int i=0;i<n;i++) sym+=expr[2*i];
    for(int i=0;i<n-1;i++) ops+=expr[2*i+1];
    vector<vector<long long>> T(n,vector<long long>(n,0)),F(n,vector<long long>(n,0));
    for(int i=0;i<n;i++){T[i][i]=(sym[i]=='T');F[i][i]=(sym[i]=='F');}
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1;
            for(int k=i;k<j;k++){
                long long lt=T[i][k],lf=F[i][k],rt=T[k+1][j],rf=F[k+1][j];
                char op=ops[k];
                if(op=='|'){T[i][j]+=lt*rt+lt*rf+lf*rt;F[i][j]+=lf*rf;}
                else if(op=='&'){T[i][j]+=lt*rt;F[i][j]+=lf*rf+lf*rt+lt*rf;}
                else{T[i][j]+=lt*rf+lf*rt;F[i][j]+=lt*rt+lf*rf;}
            }
        }
    cout<<T[0][n-1]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    String expr=sc.next();
    int n=(expr.length()+1)/2;
    char[]sym=new char[n]; char[]ops=new char[n-1];
    for(int i=0;i<n;i++) sym[i]=expr.charAt(2*i);
    for(int i=0;i<n-1;i++) ops[i]=expr.charAt(2*i+1);
    long[][]T=new long[n][n],F=new long[n][n];
    for(int i=0;i<n;i++){T[i][i]=(sym[i]=='T')?1:0;F[i][i]=(sym[i]=='F')?1:0;}
    for(int l=2;l<=n;l++)
        for(int i=0;i<=n-l;i++){
            int j=i+l-1;
            for(int k=i;k<j;k++){
                long lt=T[i][k],lf=F[i][k],rt=T[k+1][j],rf=F[k+1][j];
                if(ops[k]=='|'){T[i][j]+=lt*rt+lt*rf+lf*rt;F[i][j]+=lf*rf;}
                else if(ops[k]=='&'){T[i][j]+=lt*rt;F[i][j]+=lf*rf+lf*rt+lt*rf;}
                else{T[i][j]+=lt*rf+lf*rt;F[i][j]+=lt*rt+lf*rf;}
            }
        }
    System.out.println(T[0][n-1]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"largest_divisible_subset_size": {
"c": r"""
#include <stdio.h>
#include <stdlib.h>
int cmp(const void*a,const void*b){return *(int*)a-*(int*)b;}
int main(){
    int n; scanf("%d",&n);
    if(n==0){printf("0\n");return 0;}
    int nums[1001],dp[1001];
    for(int i=0;i<n;i++) scanf("%d",&nums[i]);
    qsort(nums,n,sizeof(int),cmp);
    int best=1;
    for(int i=0;i<n;i++){
        dp[i]=1;
        for(int j=0;j<i;j++)
            if(nums[i]%nums[j]==0&&dp[j]+1>dp[i]) dp[i]=dp[j]+1;
        if(dp[i]>best) best=dp[i];
    }
    printf("%d\n",best);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;
int main(){
    int n; cin>>n;
    if(n==0){cout<<0<<endl;return 0;}
    vector<int> nums(n); for(int i=0;i<n;i++) cin>>nums[i];
    sort(nums.begin(),nums.end());
    vector<int> dp(n,1); int best=1;
    for(int i=0;i<n;i++){
        for(int j=0;j<i;j++)
            if(nums[i]%nums[j]==0) dp[i]=max(dp[i],dp[j]+1);
        best=max(best,dp[i]);
    }
    cout<<best<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt();
    if(n==0){System.out.println(0);return;}
    int[]nums=new int[n];
    for(int i=0;i<n;i++) nums[i]=sc.nextInt();
    java.util.Arrays.sort(nums);
    int[]dp=new int[n]; int best=1;
    for(int i=0;i<n;i++){
        dp[i]=1;
        for(int j=0;j<i;j++)
            if(nums[i]%nums[j]==0) dp[i]=Math.max(dp[i],dp[j]+1);
        best=Math.max(best,dp[i]);
    }
    System.out.println(best);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"count_smaller_numbers_after_self": {
"c": r"""
#include <stdio.h>
int nums[100001],res[100001],idx[100001],tmp[100001];
long long cnt2=0;
void ms(int*arr,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms(arr,l,m); ms(arr,m+1,r);
    int i=l,j=m+1,k=l;
    while(i<=m&&j<=r){
        if(nums[arr[i]]<=nums[arr[j]]) tmp[k++]=arr[i++];
        else{res[arr[i]]+=r-j+1; /* wait, need to count */ tmp[k++]=arr[j++];}
    }
    /* Fix: count elements from right half that are smaller */
    /* Redo without cnt2 */
    for(int x=l;x<=r;x++) arr[x]=tmp[x];
}
/* Proper merge sort on indices */
void ms2(int*arr,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms2(arr,l,m); ms2(arr,m+1,r);
    int i=l,j=m+1,k=0;
    while(i<=m&&j<=r){
        if(nums[arr[i]]<=nums[arr[j]]) {tmp[k++]=arr[i++];}
        else{
            for(int x=i;x<=m;x++) res[arr[x]]++;
            tmp[k++]=arr[j++];
        }
    }
    while(i<=m) tmp[k++]=arr[i++];
    while(j<=r) tmp[k++]=arr[j++];
    for(int x=0;x<k;x++) arr[l+x]=tmp[x];
}
int main(){
    int n; scanf("%d",&n);
    for(int i=0;i<n;i++){scanf("%d",&nums[i]);idx[i]=i;res[i]=0;}
    ms2(idx,0,n-1);
    for(int i=0;i<n;i++){if(i)printf(" ");printf("%d",res[i]);}
    printf("\n");
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
using namespace std;
vector<int> res;
vector<int> nums2;
void ms(vector<int>&idx,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms(idx,l,m); ms(idx,m+1,r);
    vector<int> tmp;
    int i=l,j=m+1;
    while(i<=m&&j<=r){
        if(nums2[idx[i]]<=nums2[idx[j]]) tmp.push_back(idx[i++]);
        else{for(int x=i;x<=m;x++) res[idx[x]]++;tmp.push_back(idx[j++]);}
    }
    while(i<=m) tmp.push_back(idx[i++]);
    while(j<=r) tmp.push_back(idx[j++]);
    for(int k=l;k<=r;k++) idx[k]=tmp[k-l];
}
int main(){
    int n; cin>>n;
    nums2.resize(n); res.resize(n,0);
    for(int i=0;i<n;i++) cin>>nums2[i];
    vector<int> idx(n); for(int i=0;i<n;i++) idx[i]=i;
    ms(idx,0,n-1);
    for(int i=0;i<n;i++){if(i)cout<<" ";cout<<res[i];}
    cout<<endl;
    return 0;
}
""",
"java": r"""
static int[]nums3,res3;
static void ms(int[]idx,int l,int r){
    if(l>=r) return;
    int m=(l+r)/2;
    ms(idx,l,m); ms(idx,m+1,r);
    int[]tmp=new int[r-l+1];
    int i=l,j=m+1,k=0;
    while(i<=m&&j<=r){
        if(nums3[idx[i]]<=nums3[idx[j]]) tmp[k++]=idx[i++];
        else{for(int x=i;x<=m;x++) res3[idx[x]]++;tmp[k++]=idx[j++];}
    }
    while(i<=m) tmp[k++]=idx[i++];
    while(j<=r) tmp[k++]=idx[j++];
    for(int x=0;x<tmp.length;x++) idx[l+x]=tmp[x];
}
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt();
    nums3=new int[n]; res3=new int[n];
    for(int i=0;i<n;i++) nums3[i]=sc.nextInt();
    int[]idx=new int[n]; for(int i=0;i<n;i++) idx[i]=i;
    ms(idx,0,n-1);
    StringBuilder sb=new StringBuilder();
    for(int i=0;i<n;i++){if(i>0)sb.append(' ');sb.append(res3[i]);}
    System.out.println(sb);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"minimum_cost_to_cut_stick": {
"c": r"""
#include <stdio.h>
#include <stdlib.h>
int c[105],dp[105][105];
int cmp(const void*a,const void*b){return *(int*)a-*(int*)b;}
int main(){
    int n,m; scanf("%d",&n); scanf("%d",&m);
    int cuts[105];
    for(int i=0;i<m;i++) scanf("%d",&cuts[i]);
    qsort(cuts,m,sizeof(int),cmp);
    c[0]=0; for(int i=0;i<m;i++) c[i+1]=cuts[i]; c[m+1]=n;
    int sz=m+2;
    for(int i=0;i<sz;i++) for(int j=0;j<sz;j++) dp[i][j]=0;
    for(int l=2;l<sz;l++)
        for(int i=0;i<sz-l;i++){
            int j=i+l; dp[i][j]=2000000000;
            for(int k=i+1;k<j;k++){
                int v=dp[i][k]+dp[k][j]+c[j]-c[i];
                if(v<dp[i][j]) dp[i][j]=v;
            }
        }
    printf("%d\n",dp[0][sz-1]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <vector>
#include <algorithm>
#include <climits>
using namespace std;
int main(){
    int n,m; cin>>n>>m;
    vector<int> cuts(m);
    for(int i=0;i<m;i++) cin>>cuts[i];
    sort(cuts.begin(),cuts.end());
    vector<int> c; c.push_back(0);
    for(int x:cuts) c.push_back(x); c.push_back(n);
    int sz=c.size();
    vector<vector<int>> dp(sz,vector<int>(sz,0));
    for(int l=2;l<sz;l++)
        for(int i=0;i<sz-l;i++){
            int j=i+l; dp[i][j]=INT_MAX;
            for(int k=i+1;k<j;k++)
                dp[i][j]=min(dp[i][j],dp[i][k]+dp[k][j]+c[j]-c[i]);
        }
    cout<<dp[0][sz-1]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    int n=sc.nextInt(),m=sc.nextInt();
    int[]cuts=new int[m];
    for(int i=0;i<m;i++) cuts[i]=sc.nextInt();
    java.util.Arrays.sort(cuts);
    int[]c=new int[m+2]; c[0]=0;
    for(int i=0;i<m;i++) c[i+1]=cuts[i]; c[m+1]=n;
    int sz=m+2;
    int[][]dp=new int[sz][sz];
    for(int l=2;l<sz;l++)
        for(int i=0;i<sz-l;i++){
            int j=i+l; dp[i][j]=Integer.MAX_VALUE;
            for(int k=i+1;k<j;k++){
                int v=dp[i][k]+dp[k][j]+c[j]-c[i];
                if(v<dp[i][j]) dp[i][j]=v;
            }
        }
    System.out.println(dp[0][sz-1]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"wildcard_matching": {
"c": r"""
#include <stdio.h>
#include <string.h>
int dp[1005][1005];
int main(){
    char s[1005],p[1005]; scanf("%s%s",s,p);
    int m=strlen(s),n=strlen(p);
    dp[0][0]=1;
    for(int j=1;j<=n;j++) dp[0][j]=(p[j-1]=='*'&&dp[0][j-1]);
    for(int i=1;i<=m;i++)
        for(int j=1;j<=n;j++){
            if(p[j-1]=='*') dp[i][j]=dp[i-1][j]||dp[i][j-1];
            else dp[i][j]=(p[j-1]=='?'||s[i-1]==p[j-1])&&dp[i-1][j-1];
        }
    printf("%s\n",dp[m][n]?"True":"False");
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
using namespace std;
int main(){
    string s,p; cin>>s>>p;
    int m=s.size(),n=p.size();
    vector<vector<bool>> dp(m+1,vector<bool>(n+1,false));
    dp[0][0]=true;
    for(int j=1;j<=n;j++) dp[0][j]=(p[j-1]=='*'&&dp[0][j-1]);
    for(int i=1;i<=m;i++)
        for(int j=1;j<=n;j++){
            if(p[j-1]=='*') dp[i][j]=dp[i-1][j]||dp[i][j-1];
            else dp[i][j]=(p[j-1]=='?'||s[i-1]==p[j-1])&&dp[i-1][j-1];
        }
    cout<<(dp[m][n]?"True":"False")<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    String s=sc.next(),p=sc.next();
    int m=s.length(),n=p.length();
    boolean[][]dp=new boolean[m+1][n+1]; dp[0][0]=true;
    for(int j=1;j<=n;j++) dp[0][j]=(p.charAt(j-1)=='*'&&dp[0][j-1]);
    for(int i=1;i<=m;i++)
        for(int j=1;j<=n;j++){
            if(p.charAt(j-1)=='*') dp[i][j]=dp[i-1][j]||dp[i][j-1];
            else dp[i][j]=(p.charAt(j-1)=='?'||s.charAt(i-1)==p.charAt(j-1))&&dp[i-1][j-1];
        }
    System.out.println(dp[m][n]?"True":"False");
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"regular_expression_matching": {
"c": r"""
#include <stdio.h>
#include <string.h>
int dp[1005][1005];
int main(){
    char s[1005],p[1005]; scanf("%s%s",s,p);
    int m=strlen(s),n=strlen(p);
    dp[0][0]=1;
    for(int j=1;j<=n;j++) dp[0][j]=(p[j-1]=='*'&&j>=2&&dp[0][j-2]);
    for(int i=1;i<=m;i++)
        for(int j=1;j<=n;j++){
            if(p[j-1]=='*'){
                dp[i][j]=dp[i][j-2]||(j>=2&&(p[j-2]=='.'||p[j-2]==s[i-1])&&dp[i-1][j]);
            } else {
                dp[i][j]=(p[j-1]=='.'||p[j-1]==s[i-1])&&dp[i-1][j-1];
            }
        }
    printf("%s\n",dp[m][n]?"True":"False");
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
using namespace std;
int main(){
    string s,p; cin>>s>>p;
    int m=s.size(),n=p.size();
    vector<vector<bool>> dp(m+1,vector<bool>(n+1,false));
    dp[0][0]=true;
    for(int j=1;j<=n;j++) dp[0][j]=(p[j-1]=='*'&&j>=2&&dp[0][j-2]);
    for(int i=1;i<=m;i++)
        for(int j=1;j<=n;j++){
            if(p[j-1]=='*')
                dp[i][j]=dp[i][j-2]||(j>=2&&(p[j-2]=='.'||p[j-2]==s[i-1])&&dp[i-1][j]);
            else
                dp[i][j]=(p[j-1]=='.'||p[j-1]==s[i-1])&&dp[i-1][j-1];
        }
    cout<<(dp[m][n]?"True":"False")<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    String s=sc.next(),p=sc.next();
    int m=s.length(),n=p.length();
    boolean[][]dp=new boolean[m+1][n+1]; dp[0][0]=true;
    for(int j=1;j<=n;j++) dp[0][j]=(p.charAt(j-1)=='*'&&j>=2&&dp[0][j-2]);
    for(int i=1;i<=m;i++)
        for(int j=1;j<=n;j++){
            if(p.charAt(j-1)=='*')
                dp[i][j]=dp[i][j-2]||(j>=2&&(p.charAt(j-2)=='.'||p.charAt(j-2)==s.charAt(i-1))&&dp[i-1][j]);
            else
                dp[i][j]=(p.charAt(j-1)=='.'||p.charAt(j-1)==s.charAt(i-1))&&dp[i-1][j-1];
        }
    System.out.println(dp[m][n]?"True":"False");
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"edit_distance_v2": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXN 1005
int dp[MAXN];
int main(){
    char s[MAXN],t[MAXN];
    if(!fgets(s,MAXN,stdin)) s[0]='\0';
    if(!fgets(t,MAXN,stdin)) t[0]='\0';
    int sl=strlen(s); while(sl>0&&(s[sl-1]=='\n'||s[sl-1]=='\r'))s[--sl]='\0';
    int tl=strlen(t); while(tl>0&&(t[tl-1]=='\n'||t[tl-1]=='\r'))t[--tl]='\0';
    for(int j=0;j<=tl;j++) dp[j]=j;
    for(int i=1;i<=sl;i++){
        int prev=dp[0]; dp[0]=i;
        for(int j=1;j<=tl;j++){
            int tmp=dp[j];
            if(s[i-1]==t[j-1]) dp[j]=prev;
            else{int m=prev<dp[j]?prev:dp[j];m=m<dp[j-1]?m:dp[j-1];dp[j]=1+m;}
            prev=tmp;
        }
    }
    printf("%d\n",dp[tl]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
using namespace std;
int main(){
    string s,t; getline(cin,s); getline(cin,t);
    int sl=s.size(),tl=t.size();
    vector<int> dp(tl+1);
    for(int j=0;j<=tl;j++) dp[j]=j;
    for(int i=1;i<=sl;i++){
        int prev=dp[0]; dp[0]=i;
        for(int j=1;j<=tl;j++){
            int tmp=dp[j];
            dp[j]=(s[i-1]==t[j-1])?prev:1+min({prev,dp[j],dp[j-1]});
            prev=tmp;
        }
    }
    cout<<dp[tl]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args) throws Exception{
    BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
    String s=br.readLine(); if(s==null)s="";
    String t=br.readLine(); if(t==null)t="";
    int sl=s.length(),tl=t.length();
    int[]dp=new int[tl+1];
    for(int j=0;j<=tl;j++) dp[j]=j;
    for(int i=1;i<=sl;i++){
        int prev=dp[0]; dp[0]=i;
        for(int j=1;j<=tl;j++){
            int tmp=dp[j];
            dp[j]=(s.charAt(i-1)==t.charAt(j-1))?prev:1+Math.min(prev,Math.min(dp[j],dp[j-1]));
            prev=tmp;
        }
    }
    System.out.println(dp[tl]);
}
""",
},

# ════════════════════════════════════════════════════════════════════════════
"palindrome_min_cuts_v2": {
"c": r"""
#include <stdio.h>
#include <string.h>
#define MAXN 1005
int ip[MAXN][MAXN],cuts[MAXN];
int main(){
    char s[MAXN]; scanf("%s",s);
    int n=strlen(s);
    if(n==0){printf("0\n");return 0;}
    for(int i=0;i<n;i++) for(int j=0;j<n;j++) ip[i][j]=0;
    for(int i=0;i<n;i++) ip[i][i]=1;
    for(int i=0;i<n-1;i++) ip[i][i+1]=(s[i]==s[i+1]);
    for(int l=3;l<=n;l++)
        for(int i=0;i<=n-l;i++){int j=i+l-1;ip[i][j]=ip[i+1][j-1]&&s[i]==s[j];}
    for(int i=0;i<=n;i++) cuts[i]=i-1;
    for(int i=0;i<n;i++){
        if(ip[0][i]){cuts[i+1]=0;continue;}
        for(int j=0;j<i;j++)
            if(ip[j+1][i]&&cuts[j+1]+1<cuts[i+1]) cuts[i+1]=cuts[j+1]+1;
    }
    printf("%d\n",cuts[n]);
    return 0;
}
""",
"cpp": r"""
#include <iostream>
#include <string>
#include <vector>
using namespace std;
int main(){
    string s; cin>>s;
    int n=s.size();
    if(n==0){cout<<0<<endl;return 0;}
    vector<vector<bool>> ip(n,vector<bool>(n,false));
    for(int i=0;i<n;i++) ip[i][i]=true;
    for(int i=0;i<n-1;i++) ip[i][i+1]=(s[i]==s[i+1]);
    for(int l=3;l<=n;l++)
        for(int i=0;i<=n-l;i++){int j=i+l-1;ip[i][j]=ip[i+1][j-1]&&s[i]==s[j];}
    vector<int> cuts(n+1);
    for(int i=0;i<=n;i++) cuts[i]=i-1;
    for(int i=0;i<n;i++){
        if(ip[0][i]){cuts[i+1]=0;continue;}
        for(int j=0;j<i;j++)
            if(ip[j+1][i]) cuts[i+1]=min(cuts[i+1],cuts[j+1]+1);
    }
    cout<<cuts[n]<<endl;
    return 0;
}
""",
"java": r"""
public static void main(String[] args){
    Scanner sc=new Scanner(System.in);
    String s=sc.next();
    int n=s.length();
    if(n==0){System.out.println(0);return;}
    boolean[][]ip=new boolean[n][n];
    for(int i=0;i<n;i++) ip[i][i]=true;
    for(int i=0;i<n-1;i++) ip[i][i+1]=(s.charAt(i)==s.charAt(i+1));
    for(int l=3;l<=n;l++)
        for(int i=0;i<=n-l;i++){int j=i+l-1;ip[i][j]=ip[i+1][j-1]&&s.charAt(i)==s.charAt(j);}
    int[]cuts=new int[n+1];
    for(int i=0;i<=n;i++) cuts[i]=i-1;
    for(int i=0;i<n;i++){
        if(ip[0][i]){cuts[i+1]=0;continue;}
        for(int j=0;j<i;j++)
            if(ip[j+1][i]) cuts[i+1]=Math.min(cuts[i+1],cuts[j+1]+1);
    }
    System.out.println(cuts[n]);
}
""",
},

}  # end SOLUTIONS dict


# ── Build the multi-language bank ────────────────────────────────────────────
def build_bank():
    with open(SRC) as f:
        src = json.load(f)

    out_problems = []
    missing = []

    for prob in src["problems"]:
        pid = prob["id"]

        # Always include the original Python problem
        out_problems.append(copy.deepcopy(prob))

        # Add C, C++, Java variants
        if pid not in SOLUTIONS:
            missing.append(pid)
            continue

        for lang in ("c", "cpp", "java"):
            code = SOLUTIONS[pid].get(lang, "").strip()
            if not code:
                missing.append(f"{pid}/{lang}")
                continue

            variant = copy.deepcopy(prob)
            variant["id"] = f"{pid}_{lang}"
            variant["language"] = lang
            variant["solutions"] = [{
                "id": f"{pid}_{lang}_accepted",
                "type": "accepted",
                "description": f"Correct solution for {prob['title']} ({lang})",
                "expected_harness_statuses": ["PASS"],
                "source_code": code,
            }]
            out_problems.append(variant)

    bank = {"harness_config": {"mode": "stdio"}, "problems": out_problems}

    with open(DST, "w") as f:
        json.dump(bank, f, indent=2)

    print(f"Written {len(out_problems)} problems to {DST}")
    if missing:
        print(f"WARNING — missing solutions: {missing}")
    return missing


if __name__ == "__main__":
    missing = build_bank()
    if missing:
        import sys; sys.exit(1)
