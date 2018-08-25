# Scripts for UGR 2016

Contains utility scripts for handling UGR dataset

Note, if using nohup, add `-u` so that output is flushed

`nohup python -u script_to_run.py &`

## Columns

- te: timestamp of the end of a flow
- td: duration of flow
- sa: source IP address
- da: destination IP address
- sp: source port
- dp: destination port
- pr: protocol
- flg: flags
- fwd: forwarding status
- stos: type of service
- pkt: packets exchanged in the flow
- byt: number of bytes

Example: 

```
te	                td	    sa	            da	            sp	    dp	    pr	flg	    fwd	stos	pkt	byt	    type
2016-06-20 00:07:11	3.324	42.219.145.241	79.28.21.23	    25	    60052	TCP	.AP.SF	0	0	    29	2172	background
2016-06-20 00:07:14	1.96	42.219.156.185	108.66.255.250	54726	25	    TCP	.APRS.	0	0	    10	827	    background
2016-06-20 00:07:24	0.172	42.91.149.234	42.219.159.90	587	    23821	TCP	.AP.S.	0	0	    4	342	    background
2016-06-20 00:07:29	0.812	42.219.156.183	187.35.0.150	48946	25	    TCP	.APRS.	0	0	    8	515	    background
2016-06-20 00:07:30	1.024	42.219.158.179	56.10.179.181	25	    61138	TCP	.AP.SF	0	0	    8	657	    background
...
```