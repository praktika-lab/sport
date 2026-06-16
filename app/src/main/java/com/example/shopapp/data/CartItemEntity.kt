package com.example.shopapp.data

import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "cart_items")
data class CartItemEntity(
    @PrimaryKey val productId: Int,
    val name: String,
    val price: Double,
    val imageUrl: String,
    val quantity: Int = 1
)
