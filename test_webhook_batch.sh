#!/bin/bash

WEBHOOK_URL="https://webhook.site/05333f4b-2656-445e-bc66-0c66601cb710"
BASE_URL="http://localhost:5001"

submit() {
  local id=$1
  local payload=$2
  result=$(curl -s -X POST "$BASE_URL/submit" \
    -H "Content-Type: application/json" \
    -d "$payload")
  echo "[$id] $result"
}

# 1. Python stdio - simple addition
submit "py-stdio-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-1\",\"language\":\"python\",\"student_code\":\"a,b=map(int,input().split())\nprint(a+b)\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"2 3\n\",\"expected\":\"5\"},{\"stdin_text\":\"10 20\n\",\"expected\":\"30\"}]}"

# 2. Python stdio - sum of array
submit "py-stdio-2" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-2\",\"language\":\"python\",\"student_code\":\"n=int(input())\nprint(sum(map(int,input().split())))\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3\n1 2 3\n\",\"expected\":\"6\"},{\"stdin_text\":\"4\n10 20 30 40\n\",\"expected\":\"100\"}]}"

# 3. Python stdio - max of array
submit "py-stdio-3" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-3\",\"language\":\"python\",\"student_code\":\"n=int(input())\nprint(max(map(int,input().split())))\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3\n1 5 3\n\",\"expected\":\"5\"},{\"stdin_text\":\"4\n10 20 30 40\n\",\"expected\":\"40\"}]}"

# 4. Python stdio - reverse string
submit "py-stdio-4" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-4\",\"language\":\"python\",\"student_code\":\"print(input()[::-1])\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"hello\n\",\"expected\":\"olleh\"},{\"stdin_text\":\"world\n\",\"expected\":\"dlrow\"}]}"

# 5. Python stdio - factorial
submit "py-stdio-5" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-5\",\"language\":\"python\",\"student_code\":\"import math\nn=int(input())\nprint(math.factorial(n))\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"5\n\",\"expected\":\"120\"},{\"stdin_text\":\"6\n\",\"expected\":\"720\"}]}"

# 6. Python stdio - palindrome check
submit "py-stdio-6" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-6\",\"language\":\"python\",\"student_code\":\"s=input()\nprint('YES' if s==s[::-1] else 'NO')\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"racecar\n\",\"expected\":\"YES\"},{\"stdin_text\":\"hello\n\",\"expected\":\"NO\"}]}"

# 7. Python stdio - count vowels
submit "py-stdio-7" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-7\",\"language\":\"python\",\"student_code\":\"s=input()\nprint(sum(1 for c in s if c in 'aeiouAEIOU'))\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"hello\n\",\"expected\":\"2\"},{\"stdin_text\":\"aeiou\n\",\"expected\":\"5\"}]}"

# 8. Python stdio - multiplication
submit "py-stdio-8" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-8\",\"language\":\"python\",\"student_code\":\"a,b=map(int,input().split())\nprint(a*b)\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3 4\n\",\"expected\":\"12\"},{\"stdin_text\":\"7 8\n\",\"expected\":\"56\"}]}"

# 9. Python stdio - even or odd
submit "py-stdio-9" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-9\",\"language\":\"python\",\"student_code\":\"n=int(input())\nprint('Even' if n%2==0 else 'Odd')\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"4\n\",\"expected\":\"Even\"},{\"stdin_text\":\"7\n\",\"expected\":\"Odd\"}]}"

# 10. Python stdio - fibonacci
submit "py-stdio-10" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-10\",\"language\":\"python\",\"student_code\":\"n=int(input())\na,b=0,1\nfor _ in range(n):a,b=b,a+b\nprint(a)\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"5\n\",\"expected\":\"5\"},{\"stdin_text\":\"10\n\",\"expected\":\"55\"}]}"

# 11. C stdio - addition
submit "c-stdio-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-11\",\"language\":\"c\",\"student_code\":\"#include<stdio.h>\nint main(){int a,b;scanf(\\\"%d %d\\\",&a,&b);printf(\\\"%d\\\\n\\\",a+b);return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"2 3\n\",\"expected\":\"5\"},{\"stdin_text\":\"10 20\n\",\"expected\":\"30\"}]}"

# 12. C stdio - multiplication
submit "c-stdio-2" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-12\",\"language\":\"c\",\"student_code\":\"#include<stdio.h>\nint main(){int a,b;scanf(\\\"%d %d\\\",&a,&b);printf(\\\"%d\\\\n\\\",a*b);return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3 4\n\",\"expected\":\"12\"},{\"stdin_text\":\"7 8\n\",\"expected\":\"56\"}]}"

# 13. C stdio - sum of array
submit "c-stdio-3" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-13\",\"language\":\"c\",\"student_code\":\"#include<stdio.h>\nint main(){int n,s=0,x;scanf(\\\"%d\\\",&n);for(int i=0;i<n;i++){scanf(\\\"%d\\\",&x);s+=x;}printf(\\\"%d\\\\n\\\",s);return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3\n1 2 3\n\",\"expected\":\"6\"},{\"stdin_text\":\"4\n10 20 30 40\n\",\"expected\":\"100\"}]}"

# 14. C stdio - max of array
submit "c-stdio-4" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-14\",\"language\":\"c\",\"student_code\":\"#include<stdio.h>\nint main(){int n,m,x;scanf(\\\"%d\\\",&n);scanf(\\\"%d\\\",&m);for(int i=1;i<n;i++){scanf(\\\"%d\\\",&x);if(x>m)m=x;}printf(\\\"%d\\\\n\\\",m);return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3\n1 5 3\n\",\"expected\":\"5\"},{\"stdin_text\":\"4\n10 40 20 30\n\",\"expected\":\"40\"}]}"

# 15. C stdio - even or odd
submit "c-stdio-5" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-15\",\"language\":\"c\",\"student_code\":\"#include<stdio.h>\nint main(){int n;scanf(\\\"%d\\\",&n);printf(\\\"%s\\\\n\\\",n%2==0?\\\"Even\\\":\\\"Odd\\\");return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"4\n\",\"expected\":\"Even\"},{\"stdin_text\":\"7\n\",\"expected\":\"Odd\"}]}"

# 16. C++ stdio - addition
submit "cpp-stdio-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-16\",\"language\":\"cpp\",\"student_code\":\"#include<iostream>\nusing namespace std;\nint main(){int a,b;cin>>a>>b;cout<<a+b<<endl;return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"2 3\n\",\"expected\":\"5\"},{\"stdin_text\":\"10 20\n\",\"expected\":\"30\"}]}"

# 17. C++ stdio - multiplication
submit "cpp-stdio-2" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-17\",\"language\":\"cpp\",\"student_code\":\"#include<iostream>\nusing namespace std;\nint main(){int a,b;cin>>a>>b;cout<<a*b<<endl;return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3 4\n\",\"expected\":\"12\"},{\"stdin_text\":\"7 8\n\",\"expected\":\"56\"}]}"

# 18. C++ stdio - sum of array
submit "cpp-stdio-3" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-18\",\"language\":\"cpp\",\"student_code\":\"#include<iostream>\nusing namespace std;\nint main(){int n,s=0,x;cin>>n;for(int i=0;i<n;i++){cin>>x;s+=x;}cout<<s<<endl;return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3\n1 2 3\n\",\"expected\":\"6\"},{\"stdin_text\":\"4\n10 20 30 40\n\",\"expected\":\"100\"}]}"

# 19. C++ stdio - reverse string
submit "cpp-stdio-4" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-19\",\"language\":\"cpp\",\"student_code\":\"#include<iostream>\n#include<algorithm>\nusing namespace std;\nint main(){string s;cin>>s;reverse(s.begin(),s.end());cout<<s<<endl;return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"hello\n\",\"expected\":\"olleh\"},{\"stdin_text\":\"world\n\",\"expected\":\"dlrow\"}]}"

# 20. C++ stdio - even or odd
submit "cpp-stdio-5" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-20\",\"language\":\"cpp\",\"student_code\":\"#include<iostream>\nusing namespace std;\nint main(){int n;cin>>n;cout<<(n%2==0?\\\"Even\\\":\\\"Odd\\\")<<endl;return 0;}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"4\n\",\"expected\":\"Even\"},{\"stdin_text\":\"7\n\",\"expected\":\"Odd\"}]}"

# 21. Java stdio - addition
submit "java-stdio-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-21\",\"language\":\"java\",\"student_code\":\"import java.util.Scanner;\npublic class Student{public static void main(String[] a){Scanner sc=new Scanner(System.in);int x=sc.nextInt(),y=sc.nextInt();System.out.println(x+y);}}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"2 3\n\",\"expected\":\"5\"},{\"stdin_text\":\"10 20\n\",\"expected\":\"30\"}]}"

# 22. Java stdio - multiplication
submit "java-stdio-2" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-22\",\"language\":\"java\",\"student_code\":\"import java.util.Scanner;\npublic class Student{public static void main(String[] a){Scanner sc=new Scanner(System.in);int x=sc.nextInt(),y=sc.nextInt();System.out.println(x*y);}}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3 4\n\",\"expected\":\"12\"},{\"stdin_text\":\"7 8\n\",\"expected\":\"56\"}]}"

# 23. Java stdio - sum of array
submit "java-stdio-3" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-23\",\"language\":\"java\",\"student_code\":\"import java.util.Scanner;\npublic class Student{public static void main(String[] a){Scanner sc=new Scanner(System.in);int n=sc.nextInt(),s=0;for(int i=0;i<n;i++)s+=sc.nextInt();System.out.println(s);}}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"3\n1 2 3\n\",\"expected\":\"6\"},{\"stdin_text\":\"4\n10 20 30 40\n\",\"expected\":\"100\"}]}"

# 24. Java stdio - reverse string
submit "java-stdio-4" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-24\",\"language\":\"java\",\"student_code\":\"import java.util.Scanner;\npublic class Student{public static void main(String[] a){Scanner sc=new Scanner(System.in);String s=sc.next();System.out.println(new StringBuilder(s).reverse());}}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"hello\n\",\"expected\":\"olleh\"},{\"stdin_text\":\"world\n\",\"expected\":\"dlrow\"}]}"

# 25. Java stdio - even or odd
submit "java-stdio-5" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-25\",\"language\":\"java\",\"student_code\":\"import java.util.Scanner;\npublic class Student{public static void main(String[] a){Scanner sc=new Scanner(System.in);int n=sc.nextInt();System.out.println(n%2==0?\\\"Even\\\":\\\"Odd\\\");}}\",\"mode\":\"stdio\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"stdin_text\":\"4\n\",\"expected\":\"Even\"},{\"stdin_text\":\"7\n\",\"expected\":\"Odd\"}]}"

# 26. Python function - addition
submit "py-fn-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-26\",\"language\":\"python\",\"student_code\":\"def solve(a,b):\n    return a+b\",\"mode\":\"function\",\"function_name\":\"solve\",\"param_types\":[\"int\",\"int\"],\"return_type\":\"int\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"inputs\":[2,3],\"expected\":\"5\"},{\"inputs\":[10,20],\"expected\":\"30\"}]}"

# 27. Python function - multiplication
submit "py-fn-2" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-27\",\"language\":\"python\",\"student_code\":\"def solve(a,b):\n    return a*b\",\"mode\":\"function\",\"function_name\":\"solve\",\"param_types\":[\"int\",\"int\"],\"return_type\":\"int\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"inputs\":[3,4],\"expected\":\"12\"},{\"inputs\":[7,8],\"expected\":\"56\"}]}"

# 28. Python function - string reverse
submit "py-fn-3" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-28\",\"language\":\"python\",\"student_code\":\"def solve(s):\n    return s[::-1]\",\"mode\":\"function\",\"function_name\":\"solve\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"inputs\":[\"hello\"],\"expected\":\"olleh\"},{\"inputs\":[\"world\"],\"expected\":\"dlrow\"}]}"

# 29. C function - addition
submit "c-fn-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-29\",\"language\":\"c\",\"student_code\":\"int solve(int a,int b){return a+b;}\",\"mode\":\"function\",\"function_name\":\"solve\",\"param_types\":[\"int\",\"int\"],\"return_type\":\"int\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"inputs\":[2,3],\"expected\":\"5\"},{\"inputs\":[10,20],\"expected\":\"30\"}]}"

# 30. C++ function - addition
submit "cpp-fn-1" "{\"student_id\":\"u1\",\"assessment_id\":\"batch-30\",\"language\":\"cpp\",\"student_code\":\"int solve(int a,int b){return a+b;}\",\"mode\":\"function\",\"function_name\":\"solve\",\"param_types\":[\"int\",\"int\"],\"return_type\":\"int\",\"callback_url\":\"$WEBHOOK_URL\",\"test_cases\":[{\"inputs\":[2,3],\"expected\":\"5\"},{\"inputs\":[10,20],\"expected\":\"30\"}]}"

echo "All 30 submitted."
