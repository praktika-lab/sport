package com.example.shopapp.ui

sealed class Screen(val route: String) {
    object Catalog : Screen("catalog")
    object Favorites : Screen("favorites")
    object Cart : Screen("cart")
    object Detail : Screen("detail/{productId}") {
        fun createRoute(productId: Int) = "detail/$productId"
    }
}
