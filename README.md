# Setup

### Clone or download repository
`https://github.com/jcwhittier/Simple-Stream-Processor.git`

`cd Simple-Stream-Processor/StreamProcessor`

### Install all required packages:

`pip3 -r requirements.txt`


# Run

### Processing data in a file
Run StreamProcessor.py reading in the input file `../InputFiles/Runners_runners=1.csv`

`python3 StreamProcessor.py -f ../InputFiles/Runners_runners=1.csv`


### Processing data from STDIN
Run StreamProcessor.py reading from the `STDIN`/console 

`python3 StreamProcessor.py -s`

You may pipe in or enter data manually as shown below:
 
`1,sensors,-68.66724609375, 44.90699338679709,0,114`<br>
`1,sensors,-68.66724609375, 44.90687087743804,1,108`<br>
`1,sensors,-68.66724609375, 44.90674333301809,2,108`<br>
`1,sensors,-68.66724609375, 44.90658791174276,3,109`<br>
`1,sensors,-68.66724609375, 44.90640533882305,4,110`<br>
`1,sensors,-68.66724609375, 44.906258659796144,5,104`<br>
`1,sensors,-68.66724609375, 44.906110298437966,6,113`<br>
`1,sensors,-68.66724609375, 44.90592964247795,7,112`<br>
`1,sensors,-68.66724609375, 44.905740970542475,8,110`<br>
`1,sensors,-68.66724609375, 44.90561240278874,9,115`<br>

### Processing data from a connected Arduino (untested)
Run StreamProcessor.py reading data from an Arduino connected as a serial device 

Windows example:<br>
`python3 StreamProcessor.py -p COM3`

MacOS example:<br>
`python3 StreamProcessor.py -p /dev/cu.usbmodemFD13131`
