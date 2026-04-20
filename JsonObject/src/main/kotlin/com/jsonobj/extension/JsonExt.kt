package com.jsonobj.extension

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken

val gson: Gson =
    GsonBuilder()
        .serializeNulls()
        .create()

fun Any?.replaceNullsRecursively(): Any? =
    when (this) {
        null -> null
        is Map<*, *> -> this.mapValues { (_, value) -> value.replaceNullsRecursively() }
        is List<*> -> this.map { it.replaceNullsRecursively() }
        else -> this
    }

fun <T> T.serializeToMap(): Map<String, Any?> {
    val json = gson.toJson(this)
    val map: Map<String, Any?> = gson.fromJson(json, object : TypeToken<Map<String, Any?>>() {}.type)
    return map.replaceNullsRecursively() as Map<String, Any?>
}

inline fun <reified T> Map<String, Any?>.toDataClass(): T {
    val json = gson.toJson(this)
    return gson.fromJson(json, T::class.java)
}
