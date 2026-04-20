package model

import kotlinx.serialization.Serializable

@Serializable
data class ImageInfo(
    val customerId: String,
    val jsonPath: String,
)
