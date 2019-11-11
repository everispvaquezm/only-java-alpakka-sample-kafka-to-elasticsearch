To execute this little demo, 

1) Start your docker local installation
2) Go to the directory where this README.md file is located an issue the following command: 

mvn clean compile exec:java -Dexec.mainClass=samples.javadsl.Main

It's issuing a final exception but I think is reading from kafka and writing to Elastic search ...
 
