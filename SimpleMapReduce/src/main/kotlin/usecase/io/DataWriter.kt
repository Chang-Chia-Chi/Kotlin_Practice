package usecase.io

import java.io.File
import java.util.concurrent.ConcurrentHashMap

interface DataWriter<T> {
    val fileMap: ConcurrentHashMap<String, File>

    fun open(name: String): String {
        fileMap[name] = File(name)
        return name
    }

    fun close(name: String)

    fun write(
        name: String,
        obj: T,
    )

    fun flush()
}
