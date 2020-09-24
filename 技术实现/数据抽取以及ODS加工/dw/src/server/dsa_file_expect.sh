#!/usr/bin/expect
set timeout 30
set user [lindex $argv 0]
set passwd [lindex $argv 1]
set ip [lindex $argv 2]
set sdir [lindex $argv 3]
set sys [lindex $argv 4]
set date [lindex $argv 5]
set tdir [lindex $argv 6]
spawn sftp $user@$ip
expect -re "(.*)(yes/no)?" 
send "yes \r";
expect "*password:"
send "$passwd\r"
expect "sftp>"
send "cd $tdir\r"
expect "sftp>"
send "put $sdir/$date.tar.gz \r"
expect "sftp>"
send "put $sdir/$sys.ok \r"
expect "sftp>"
send "bye \r"
expect eof

