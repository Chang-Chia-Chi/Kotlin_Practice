package org.example

import kotlinx.coroutines.*
import kotlin.system.measureTimeMillis

suspend fun <R1, R2, R> (suspend () -> R1).zipWith(
    zip: suspend () -> R2,
    combine: suspend (R1, R2) -> R,
): R =
    coroutineScope {
        val deferred1 = async { this@zipWith() }
        val deferred2 = async { zip() }

        try {
            val result1 = deferred1.await()
            val result2 = deferred2.await()
            combine(result1, result2)
        } catch (e: Exception) {
            throw e
        }
    }

suspend fun fetchData1(data: String): String {
    delay(1000)
    return data
}

suspend fun fetchData2(data: Int): Int {
    delay(500)
    return data
}

suspend fun combineData(
    data1: String,
    data2: Int,
): String = "$data1 + $data2"

fun main() =
    runBlocking {
        val millis =
            measureTimeMillis {
                try {
                    val combinedResult =
                        suspendLambda { fetchData1("Data1") }
                            .zipWith({ fetchData2(2) }, ::combineData)
                    println("Combined Result: $combinedResult")
                } catch (e: Exception) {
                    println("An error occurred: ${e.message}")
                }
            }
        println(millis)
    }

private fun <T> suspendLambda(block: suspend () -> T): suspend () -> T = block
