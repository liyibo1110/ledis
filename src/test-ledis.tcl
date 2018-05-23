#自动化测试脚本
set ::passed 0  ;#全局统计数
set ::failed 0  ;#全局统计数

proc test {name code okpattern} {
    puts -nonewline [format "%-70s " name]
    flush stdout
    set retval [uplevel 1 $code]    ;#在全局环境执行code
    if {$okpattern eq $retval || [string match $okpattern $retval]} {
        puts "PASSED"
        incr ::passed
    } else {
        puts "!! ERROR expected '$okpattern' but got '$retval'"
        incr ::failed
    }
}

proc main {server port} {
    set fd [ledis_connect $server $port]
    puts "\n[expr $::passed+$::failed] tests, $::passed passed, $::failed failed"
    if{$::failed > 0} {
        puts "\n*** WARNING!!! $::failed FAILED TESTS ***\n"
    }
    close $fd
}

proc ledis_connect {server port} {
    set fd [socket $server $port]
    fconfigure $fd -translation binary
    return $fd
}

#在fd中输出buf不换行
proc ledis_write {fd buf} {
    puts -nonewline $fd $buf
}

#在fd中输出buf并换行
proc ledis_writenl {fd buf} {
    ledis_write $fd $buf
    ledis_write $fd "\r\n"
    flush $fd
}

#开始执行
if{[llength $argv] == 0}{
    main 127.0.0.1 6379
} else {
    main [lindex $argv 0] [lindex $argv 1]
}