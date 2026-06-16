package com.example.shopapp.ui

import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Favorite
import androidx.compose.material.icons.filled.Home
import androidx.compose.material.icons.filled.ShoppingCart
import androidx.compose.material3.Badge
import androidx.compose.material3.BadgedBox
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.navigation.NavType
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import androidx.navigation.navArgument
import com.example.shopapp.data.ProductRepository
import com.example.shopapp.ui.screens.CartScreen
import com.example.shopapp.ui.screens.CatalogScreen
import com.example.shopapp.ui.screens.DetailScreen
import com.example.shopapp.ui.screens.FavoritesScreen

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ShopApp(
    viewModel: ShopViewModel,
    repository: ProductRepository
) {
    val navController = rememberNavController()
    val navBackStackEntry by navController.currentBackStackEntryAsState()
    val currentRoute = navBackStackEntry?.destination?.route

    val catalogState by viewModel.catalogUiState.collectAsState()
    val favoritesState by viewModel.favoritesState.collectAsState()
    val cartState by viewModel.cartUiState.collectAsState()
    val cartBadge by viewModel.cartBadgeCount.collectAsState()

    // Скрываем нижнюю панель на экране детали товара
    val showBottomBar = currentRoute?.startsWith("detail") == false

    val topBarTitle = when {
        currentRoute == Screen.Catalog.route  -> "Каталог"
        currentRoute == Screen.Favorites.route -> "Избранное"
        currentRoute == Screen.Cart.route      -> "Корзина"
        else -> ""
    }

    Scaffold(
        topBar = {
            if (showBottomBar) {
                TopAppBar(
                    title = { Text(topBarTitle) },
                    colors = TopAppBarDefaults.topAppBarColors(
                        containerColor = MaterialTheme.colorScheme.surface
                    )
                )
            }
        },
        bottomBar = {
            if (showBottomBar) {
                NavigationBar {
                    NavigationBarItem(
                        selected = currentRoute == Screen.Catalog.route,
                        onClick = {
                            navController.navigate(Screen.Catalog.route) {
                                popUpTo(Screen.Catalog.route) { inclusive = true }
                            }
                        },
                        icon = { Icon(Icons.Default.Home, contentDescription = null) },
                        label = { Text("Каталог") }
                    )
                    NavigationBarItem(
                        selected = currentRoute == Screen.Favorites.route,
                        onClick = {
                            navController.navigate(Screen.Favorites.route) {
                                popUpTo(Screen.Catalog.route)
                            }
                        },
                        icon = { Icon(Icons.Default.Favorite, contentDescription = null) },
                        label = { Text("Избранное") }
                    )
                    NavigationBarItem(
                        selected = currentRoute == Screen.Cart.route,
                        onClick = {
                            navController.navigate(Screen.Cart.route) {
                                popUpTo(Screen.Catalog.route)
                            }
                        },
                        icon = {
                            BadgedBox(
                                badge = {
                                    if (cartBadge > 0) {
                                        Badge { Text(cartBadge.toString()) }
                                    }
                                }
                            ) {
                                Icon(Icons.Default.ShoppingCart, contentDescription = null)
                            }
                        },
                        label = { Text("Корзина") }
                    )
                }
            }
        }
    ) { innerPadding ->
        NavHost(
            navController = navController,
            startDestination = Screen.Catalog.route,
            modifier = Modifier.padding(innerPadding)
        ) {
            composable(Screen.Catalog.route) {
                CatalogScreen(
                    state = catalogState,
                    onSearchChange = viewModel::setSearchQuery,
                    onSelectCategory = viewModel::selectCategory,
                    onToggleFavorite = viewModel::toggleFavorite,
                    onAddToCart = viewModel::addToCart,
                    onOpenDetail = { id ->
                        navController.navigate(Screen.Detail.createRoute(id))
                    }
                )
            }

            composable(Screen.Favorites.route) {
                FavoritesScreen(
                    favorites = favoritesState,
                    onRemoveFavorite = viewModel::toggleFavorite,
                    onAddToCart = viewModel::addToCart,
                    onOpenDetail = { id ->
                        navController.navigate(Screen.Detail.createRoute(id))
                    }
                )
            }

            composable(Screen.Cart.route) {
                CartScreen(
                    state = cartState,
                    onIncrease = viewModel::increaseQuantity,
                    onDecrease = viewModel::decreaseQuantity,
                    onRemove = viewModel::removeFromCart,
                    onPlaceOrder = viewModel::placeOrder,
                    onOrderConfirmed = viewModel::resetOrderFlag
                )
            }

            composable(
                route = Screen.Detail.route,
                arguments = listOf(navArgument("productId") { type = NavType.IntType })
            ) { backStackEntry ->
                val productId = backStackEntry.arguments?.getInt("productId") ?: return@composable
                DetailScreen(
                    productId = productId,
                    repository = repository,
                    onBack = { navController.popBackStack() },
                    onAddToCart = viewModel::addToCart,
                    onToggleFavorite = viewModel::toggleFavorite
                )
            }
        }
    }
}
