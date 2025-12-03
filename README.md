# Backyard Birds Application - Shane McVeigh

# Project Write-Up & Overview: 
Located in this repo as [Final Project Write-Up Shane McVeigh.pdf](https://github.com/shanemcv/backyard-birds/blob/main/Final%20Project%20Write-Up%20Shane%20McVeigh.pdf)

# Project Video Walkthrough
Located in this repo as [shane mcveigh video](https://google.com)

# Run Instructions:
## (WebServer) 
ssh ec2-user@ec2-52-20-203-80.compute-1.amazonaws.com

cd smcveigh/birdapp

node app.js 3007 http://ec2-34-230-47-10.compute-1.amazonaws.com:8070

## (Speed Layer) 
ssh hadoop@ec2-34-230-47-10.compute-1.amazonaws.com

cd smcveigh/smcveigh-speed-layer-birds/

spark-submit --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/log4j.properties" --master local[2] --class smcveigh.StreamBirdObservations target/uber-smcveigh-speed-layer-birds-1.0-SNAPSHOT.jar boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196

## (Website) 
Browse to 

(a) http://ec2-52-20-203-80.compute-1.amazonaws.com:3007/index.html [For viewing bird by state results]

(b) http://ec2-52-20-203-80.compute-1.amazonaws.com:3007/biome.html [For viewing bird by biome results]

(c) http://ec2-52-20-203-80.compute-1.amazonaws.com:3007/submit-birds.html 

[For submitting bird observations to speed layer, this can be combined with (a) to view updates in real time!]

## (Monitor Kafka Topic)
ssh hadoop@ec2-34-230-47-10.compute-1.amazonaws.com

cd kafka/bin/

kafka-console-consumer.sh --bootstrap-server boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196 --consumer.config ~/kafka.client.properties --topic smcveigh_bird_observations --from-beginning
