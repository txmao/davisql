$ davisql, A Rudimentary Database Engine $

# File description
  source codes are within davisql/src directory:
    1. DavisBasePrompt.java is the source codes that pass commands in terminal to the engine;
    2. Table.java is the core codes for data definition, manipulation;
    3. Displayer.java is used to catch and view the data
    4. davisql.java the the top level class for the whole program
    
# How to compile & run (using Linux as example)
  - Before compile and run the program, make sure you installed Java
  cd davisql
  sh run.sh
  
# The design document is in davisql/doc directory
# clr.sh is used to clear the data directory (initialization)

Note: When inserting new records, the insert position (determined by keys) is wrong in this implementation.
      Therefore there is insert error sometimes.
