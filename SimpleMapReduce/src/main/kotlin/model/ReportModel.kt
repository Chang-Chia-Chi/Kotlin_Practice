package model

data class ReportModel(
    val city: String = "",
) {
    fun key(): String = this.city
}
