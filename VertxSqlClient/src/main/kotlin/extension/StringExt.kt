package extension

fun List<String>.toWhereClause(): String = joinToString(", ") { "'$it'" }
