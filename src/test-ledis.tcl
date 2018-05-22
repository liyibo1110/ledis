#自动化测试脚本
set ::passed 0  ;#全局统计数
set ::failed 0  ;#全局统计数

proc main {server port} {
    #set fd [ledis_connect $server $port]
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

#开始执行
if{[llength $argv] == 0}{
    main 127.0.0.1 6379
} else {
    main [lindex $argv 0] [lindex $argv 1]
}