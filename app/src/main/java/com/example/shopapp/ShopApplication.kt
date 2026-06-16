package com.example.shopapp

import android.app.Application
import com.example.shopapp.data.ProductRepository
import com.example.shopapp.data.ShopDatabase
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

class ShopApplication : Application() {

    private val applicationScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    val repository: ProductRepository by lazy {
        val db = ShopDatabase.buildDatabase(this)
        ProductRepository(db.productDao(), db.cartDao())
    }

    override fun onCreate() {
        super.onCreate()
        applicationScope.launch {
            repository.seedIfEmpty()
        }
    }
}
