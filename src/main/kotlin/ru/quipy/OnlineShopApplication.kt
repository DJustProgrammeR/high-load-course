package ru.quipy

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import ru.quipy.common.utils.NamedThreadFactory
import java.util.concurrent.Executors

//import java.net.InetSocketAddress
//import java.net.Socket

@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    companion object {
        val appExecutor = Executors.newFixedThreadPool(64, NamedThreadFactory("main-app-executor"))
    }
}

fun main(args: Array<String>) {
//    System.out.println(measureConnectionTime("127.0.0.1", 1234))
    runApplication<OnlineShopApplication>(*args)
}
//
//fun measureConnectionTime(host: String, port: Int): Long {
//    var  c  = 0L;
//    for (i in 0 .. 400) {
//        val start = System.nanoTime()
//
//        Socket().use { socket ->
//            socket.connect(InetSocketAddress(host, port))
//        }
//        c = c +  System.nanoTime() - start
//    }
//
//
//    return c/400L
//}
