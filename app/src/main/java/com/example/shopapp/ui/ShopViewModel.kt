package com.example.shopapp.ui

import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import com.example.shopapp.data.CartItemEntity
import com.example.shopapp.data.ProductEntity
import com.example.shopapp.data.ProductRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

// ─── UI State ────────────────────────────────────────────────────────────────

data class CatalogUiState(
    val products: List<ProductEntity> = emptyList(),
    val categories: List<String> = emptyList(),
    val selectedCategory: String? = null,
    val searchQuery: String = ""
)

data class CartUiState(
    val items: List<CartItemEntity> = emptyList(),
    val totalPrice: Double = 0.0,
    val totalCount: Int = 0,
    val orderPlaced: Boolean = false
)

// ─── ViewModel ───────────────────────────────────────────────────────────────

@OptIn(ExperimentalCoroutinesApi::class)
class ShopViewModel(
    private val repository: ProductRepository
) : ViewModel() {

    private val selectedCategory = MutableStateFlow<String?>(null)
    private val searchQuery = MutableStateFlow("")
    private val orderPlacedFlag = MutableStateFlow(false)

    val catalogUiState: StateFlow<CatalogUiState> = combine(
        selectedCategory.flatMapLatest { cat ->
            if (cat == null) repository.observeAll()
            else repository.observeByCategory(cat)
        },
        repository.observeAll(),
        selectedCategory,
        searchQuery
    ) { filtered, all, cat, query ->
        val categories = all.map { it.category }.distinct().sorted()
        val displayed = if (query.isBlank()) filtered
        else filtered.filter {
            it.name.contains(query, ignoreCase = true) ||
                    it.description.contains(query, ignoreCase = true)
        }
        CatalogUiState(
            products = displayed,
            categories = categories,
            selectedCategory = cat,
            searchQuery = query
        )
    }.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5_000),
        initialValue = CatalogUiState()
    )

    val favoritesState: StateFlow<List<ProductEntity>> = repository.observeFavorites()
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = emptyList()
        )

    val cartUiState: StateFlow<CartUiState> = combine(
        repository.observeCartItems(),
        repository.observeCartTotal(),
        repository.observeCartCount(),
        orderPlacedFlag
    ) { items, total, count, placed ->
        CartUiState(
            items = items,
            totalPrice = total ?: 0.0,
            totalCount = count ?: 0,
            orderPlaced = placed
        )
    }.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5_000),
        initialValue = CartUiState()
    )

    val cartBadgeCount: StateFlow<Int> = repository.observeCartCount()
        .map { it ?: 0 }
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = 0
        )

    // ─── Каталог ─────────────────────────────────────────────────────────────

    fun selectCategory(category: String?) {
        selectedCategory.value = category
    }

    fun setSearchQuery(query: String) {
        searchQuery.value = query
    }

    fun toggleFavorite(product: ProductEntity) {
        viewModelScope.launch {
            repository.toggleFavorite(product)
        }
    }

    fun addToCart(product: ProductEntity) {
        viewModelScope.launch {
            repository.addToCart(product)
        }
    }

    // ─── Корзина ─────────────────────────────────────────────────────────────

    fun increaseQuantity(item: CartItemEntity) {
        viewModelScope.launch {
            repository.increaseQuantity(item)
        }
    }

    fun decreaseQuantity(item: CartItemEntity) {
        viewModelScope.launch {
            repository.decreaseQuantity(item)
        }
    }

    fun removeFromCart(productId: Int) {
        viewModelScope.launch {
            repository.removeFromCart(productId)
        }
    }

    fun placeOrder() {
        viewModelScope.launch {
            repository.clearCart()
            orderPlacedFlag.value = true
        }
    }

    fun resetOrderFlag() {
        orderPlacedFlag.value = false
    }
}

// ─── Factory ─────────────────────────────────────────────────────────────────

class ShopViewModelFactory(
    private val repository: ProductRepository
) : ViewModelProvider.Factory {
    @Suppress("UNCHECKED_CAST")
    override fun <T : ViewModel> create(modelClass: Class<T>): T {
        if (modelClass.isAssignableFrom(ShopViewModel::class.java)) {
            return ShopViewModel(repository) as T
        }
        throw IllegalArgumentException("Unknown ViewModel class")
    }
}
