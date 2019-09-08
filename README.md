# scala-ex

## Description

This code calculates some statistics about payloads from different domain based on data in the inputFile. The path to inputFile is hard-coded in the code with the variable name inputFilePath. The output will be a txt file in the outputDirectory, which is hard-code with the variable name outputDirPath. The The format of the input file will be:

```
http://subdom0001.example.com,/endpoint0001,POST,3B
http://subdom0002.example.com,/endpoint0002,GET,431MB
http://subdom0003.example.com,/endpoint0003,POST,231KB
http://subdom0002.example.com,/endpoint0002,GET,29MB
http://subdom0001.example.com,/endpoint0001,POST,238B
http://subdom0002.example.com,/endpoint0001,GET,32MB
http://subdom0003.example.com,/endpoint0003,GET,21KB
```

The format of the output file will be:

```
http://subdom0001.example.com,3B,238B,120B,13806B
http://subdom0002.example.com,30408704B,451936256B,171966464B,39193191483703296B
http://subdom0003.example.com,21504B,236544B,129024B,11560550400B
```

where the columns in the output above are as follows: Base URL, minimum payload, maximum payload, mean payload, variance of payload.
