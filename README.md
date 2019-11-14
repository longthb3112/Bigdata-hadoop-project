# Description
A sample project to practice for pair and stripe with relative frequency. 

# Install Hadoop
Use this link below to install hadoop for windows
https://www.datasciencecentral.com/profiles/blogs/how-to-install-and-run-hadoop-on-windows-for-beginners

# Note for installation
```
1 . Add paths to environment variables in windows carefully
2 . Use core-site.xml, hadoop-env.cmd, hdfs-site.xml, mapred-site.xml, yarn-site.xml as your references
3 . Use pom.xml for maven ( feel free to update hadoop version ) 
```
# Execute

There are two ways to run:
1 . Export to jar file and run with hadoop
2 . Use IDE(IntelliJ, Eclipse) to run directly

# Examples
Pair
```
Input:   B11 C31 D76 A12 B11 C31 D76 C31 A10 B12 D76 C31
		 D76 D76 B12 B11 C31 D76 B12 C31 B11 A12 C31 B12
Output: 
		(A10, B12)	0.3333333333333333
		(A10, C31)	0.3333333333333333
		(A10, D76)	0.3333333333333333
		(A12, A10)	0.1
		(A12, B11)	0.1
		(A12, B12)	0.2
		(A12, C31)	0.4
		(A12, D76)	0.2
		(B11, A10)	0.058823529411764705
		(B11, A12)	0.11764705882352941
		(B11, B12)	0.17647058823529413
		(B11, C31)	0.4117647058823529
		(B11, D76)	0.23529411764705882
		(B12, A12)	0.1111111111111111
		(B12, B11)	0.2222222222222222
		(B12, C31)	0.4444444444444444
		(B12, D76)	0.2222222222222222
		(C31, A10)	0.08333333333333333
		(C31, A12)	0.16666666666666666
		(C31, B11)	0.16666666666666666
		(C31, B12)	0.25
		(C31, D76)	0.3333333333333333
		(D76, A10)	0.0625
		(D76, A12)	0.125
		(D76, B11)	0.1875
		(D76, B12)	0.25
		(D76, C31)	0.375

```

Stripe
```
Input:  B11 C31 D76 A12 B11 C31 D76 C31 A10 B12 D76 C31
		D76 D76 B12 B11 C31 D76 B12 C31 B11 A12 C31 B12
		
Output: 
		A10	{C31=0.3333333333333333, D76=0.3333333333333333, B12=0.3333333333333333}
		A12	{B11=0.1, C31=0.4, D76=0.2, B12=0.2, A10=0.1}
		B11	{A12=0.11764705882352941, C31=0.4117647058823529, D76=0.23529411764705882, B12=0.17647058823529413, A10=0.058823529411764705}
		B12	{B11=0.2222222222222222, A12=0.1111111111111111, C31=0.4444444444444444, D76=0.2222222222222222}
		C31	{A12=0.16666666666666666, B11=0.16666666666666666, D76=0.3333333333333333, B12=0.25, A10=0.08333333333333333}
		D76	{A12=0.125, B11=0.1875, C31=0.375, B12=0.25, A10=0.0625}
```

