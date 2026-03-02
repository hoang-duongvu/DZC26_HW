## Q1: redpanda version

`docker exec redpanda-1 rpk --version`

rpk version v24.2.18 (rev f9a22d4430)

## Q2: creating a topic

`rpk topic create green-trips --partitions 3 --replicas 1`

TOPIC        STATUS
green-trips  OK

## Q3: connect to kafka server

True

## Q4: sending the trip data

47.80282402038574

## Q5: build sessionization window
 pulocationid | dolocationid | longest_trips 
--------------+--------------+---------------
           82 |          129 |            20