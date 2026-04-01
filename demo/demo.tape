Output demo.gif
Output demo.mp4

Set Shell zsh
Set FontSize 16
Set Width 1850
Set Height 700
Set TypingSpeed 50ms
Set Theme "Molokai"
Set WindowBar Rings

Sleep 500ms

Type "psql -d postgres --pset pager=off"
Enter
Sleep 2s

# Start a primary and capture port
Type "SELECT * FROM mysql.start(semi_sync => true) \gset primary_"
Enter
Sleep 4s

# Check status
Type "SELECT * FROM mysql.status();"
Enter
Sleep 4s

# Run queries on the primary
Type "SELECT mysql.query('SELECT @@version, @@version_comment');"
Enter
Sleep 3s

Type "SELECT mysql.query('SHOW DATABASES');"
Enter
Sleep 3s

# Start a replica using the captured primary port
Type "SELECT * FROM mysql.start_replica('127.0.0.1', :primary_port, semi_sync => true) \gset replica_"
Enter
Sleep 3s
Type "-- waiting for clone + replica startup..."
Enter
Sleep 12s

# Check status with both instances
Type "SELECT * FROM mysql.status();"
Enter
Sleep 4s

# Create a table, insert data, and query on the primary
Type "SELECT mysql.query('CREATE DATABASE test; USE test; CREATE TABLE test (id bigint)');"
Enter
Sleep 4s

Type "SELECT mysql.query('INSERT INTO test.test (id) VALUES(123)');"
Enter
Sleep 3s

Type "SELECT mysql.query('SELECT * FROM test.test');"
Enter
Sleep 3s

# Query the replica
Type "SELECT mysql.query('SHOW DATABASES', :replica_port);"
Enter
Sleep 3s

Type "SELECT mysql.query('SELECT * FROM test.test', :replica_port);"
Enter
Sleep 3s

# Stop the replica
Type "SELECT mysql.stop(:replica_port);"
Enter
Sleep 2s

# Delete both
Type "SELECT mysql.delete(:replica_port);"
Enter
Sleep 4s
Type "SELECT mysql.delete(:primary_port);"
Enter
Sleep 8s

# Verify clean
Type "SELECT * FROM mysql.status();"
Enter
Sleep 2s

Type "\q"
Enter
Sleep 5s
