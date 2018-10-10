int photocellPin = 0;
int ID = 0;

void setup() {
  // put your setup code here, to run once:

  // initialize the serial port 
  Serial.begin(9600);
}

void loop() {
  // put your main code here, to run repeatedly:
  delay(2000);

  // read the photocell if attached
  int photocellReading = analogRead(photocellPin);

  // format the observation
  String obs = String(ID, DEC) + "," + String(photocellReading, DEC);

  // write the observation to the serial output
  Serial.println(obs);

  // increment observation ID
  ID = ID + 1;
}
