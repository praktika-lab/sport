package com.example.shopapp

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.shopapp.ui.ShopApp
import com.example.shopapp.ui.ShopViewModel
import com.example.shopapp.ui.ShopViewModelFactory
import com.example.shopapp.ui.theme.ShopTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val repository = (application as ShopApplication).repository

        setContent {
            ShopTheme {
                val viewModel: ShopViewModel = viewModel(
                    factory = ShopViewModelFactory(repository)
                )
                ShopApp(
                    viewModel = viewModel,
                    repository = repository
                )
            }
        }
    }
}
