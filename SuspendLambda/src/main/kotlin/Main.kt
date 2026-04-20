package org.example

import kotlinx.coroutines.*

fun main() {
    getMessage { text ->
        println("process message start...")
        Thread.sleep(1000)
        println("process message done...")
    }
    println("Wait for message processing finished...")
    Thread.sleep(1000)
}

fun getMessage(onReceive: suspend (text: String) -> Unit) {
    val msg = "123"
    println("Receive message $msg...")
    CoroutineScope(Dispatchers.IO).launch {
        onReceive(msg)
        println("commit message...")
    }
}
