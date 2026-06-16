package com.example.shopapp.data

import kotlinx.coroutines.flow.Flow

class ProductRepository(
    private val productDao: ProductDao,
    private val cartDao: CartDao
) {
    // ─── Каталог ────────────────────────────────────────────────────────────

    fun observeAll(): Flow<List<ProductEntity>> = productDao.observeAll()

    fun observeByCategory(category: String): Flow<List<ProductEntity>> =
        productDao.observeByCategory(category)

    fun observeFavorites(): Flow<List<ProductEntity>> = productDao.observeFavorites()

    suspend fun toggleFavorite(product: ProductEntity) {
        productDao.setFavorite(product.id, !product.isFavorite)
    }

    suspend fun getById(id: Int): ProductEntity? = productDao.getById(id)

    // ─── Корзина ────────────────────────────────────────────────────────────

    fun observeCartItems(): Flow<List<CartItemEntity>> = cartDao.observeAll()

    fun observeCartCount(): Flow<Int?> = cartDao.observeTotalCount()

    fun observeCartTotal(): Flow<Double?> = cartDao.observeTotalPrice()

    suspend fun addToCart(product: ProductEntity) {
        val existing = cartDao.getByProductId(product.id)
        if (existing != null) {
            cartDao.update(existing.copy(quantity = existing.quantity + 1))
        } else {
            cartDao.insert(
                CartItemEntity(
                    productId = product.id,
                    name = product.name,
                    price = product.price,
                    imageUrl = product.imageUrl
                )
            )
        }
    }

    suspend fun increaseQuantity(item: CartItemEntity) {
        cartDao.update(item.copy(quantity = item.quantity + 1))
    }

    suspend fun decreaseQuantity(item: CartItemEntity) {
        if (item.quantity <= 1) {
            cartDao.deleteByProductId(item.productId)
        } else {
            cartDao.update(item.copy(quantity = item.quantity - 1))
        }
    }

    suspend fun removeFromCart(productId: Int) {
        cartDao.deleteByProductId(productId)
    }

    suspend fun clearCart() {
        cartDao.clearAll()
    }

    // ─── Seed ────────────────────────────────────────────────────────────────

    suspend fun seedIfEmpty() {
        if (productDao.count() > 0) return
        productDao.insertAll(SEED_PRODUCTS)
    }

    companion object {
        // Используем picsum.photos — стабильный источник placeholder-изображений
        private val SEED_PRODUCTS = listOf(
            ProductEntity(
                id = 1,
                name = "Смартфон UltraX 12",
                description = "6,7-дюймовый AMOLED-дисплей, 256 ГБ памяти, тройная камера 108 МП. Быстрая зарядка 67 Вт.",
                price = 49990.0,
                imageUrl = "https://picsum.photos/seed/phone1/400/400",
                category = "Электроника",
                rating = 4.7f
            ),
            ProductEntity(
                id = 2,
                name = "Ноутбук ProBook 15",
                description = "Intel Core i7, 16 ГБ ОЗУ, SSD 512 ГБ, дисплей IPS 15,6 дюйма, автономность до 10 часов.",
                price = 79990.0,
                imageUrl = "https://picsum.photos/seed/laptop1/400/400",
                category = "Электроника",
                rating = 4.5f
            ),
            ProductEntity(
                id = 3,
                name = "Беспроводные наушники SoundPro",
                description = "Активное шумоподавление, 30 ч работы от батареи, кодек aptX HD.",
                price = 8990.0,
                imageUrl = "https://picsum.photos/seed/headphones1/400/400",
                category = "Электроника",
                rating = 4.8f
            ),
            ProductEntity(
                id = 4,
                name = "Умные часы FitWatch 3",
                description = "ЭКГ, SpO2, GPS, 5 дней автономной работы. Водозащита IP68.",
                price = 14990.0,
                imageUrl = "https://picsum.photos/seed/watch1/400/400",
                category = "Электроника",
                rating = 4.3f
            ),
            ProductEntity(
                id = 5,
                name = "Кроссовки RunMax Air",
                description = "Амортизирующая подошва Air, дышащий сетчатый верх, подходят для бега и повседневной носки.",
                price = 5990.0,
                imageUrl = "https://picsum.photos/seed/shoes1/400/400",
                category = "Одежда",
                rating = 4.6f
            ),
            ProductEntity(
                id = 6,
                name = "Куртка ThermoShield",
                description = "Мембранная ткань, утеплитель 200 г/м², водонепроницаемость 10 000 мм вод. ст.",
                price = 7990.0,
                imageUrl = "https://picsum.photos/seed/jacket1/400/400",
                category = "Одежда",
                rating = 4.4f
            ),
            ProductEntity(
                id = 7,
                name = "Рюкзак CityPack 30L",
                description = "30 литров, отсек для ноутбука 15,6\", USB-порт, влагостойкий нейлон.",
                price = 3490.0,
                imageUrl = "https://picsum.photos/seed/bag1/400/400",
                category = "Аксессуары",
                rating = 4.2f
            ),
            ProductEntity(
                id = 8,
                name = "Кофемашина BrewMaster",
                description = "15 бар давления, встроенная кофемолка, резервуар 1,8 л, режимы эспрессо и капучино.",
                price = 24990.0,
                imageUrl = "https://picsum.photos/seed/coffee1/400/400",
                category = "Дом и кухня",
                rating = 4.9f
            ),
            ProductEntity(
                id = 9,
                name = "Робот-пылесос CleanBot X5",
                description = "Лазерная навигация LIDAR, мощность всасывания 3000 Па, автоматическая зарядка, совместим с Алисой.",
                price = 32990.0,
                imageUrl = "https://picsum.photos/seed/vacuum1/400/400",
                category = "Дом и кухня",
                rating = 4.6f
            ),
            ProductEntity(
                id = 10,
                name = "Настольная лампа LightDesk",
                description = "LED, 4 режима освещения, беспроводная зарядка Qi 10 Вт в основании, USB-A выход.",
                price = 2990.0,
                imageUrl = "https://picsum.photos/seed/lamp1/400/400",
                category = "Дом и кухня",
                rating = 4.1f
            ),
            ProductEntity(
                id = 11,
                name = "Планшет TabView 10",
                description = "10,5-дюймовый IPS 2K, чип Dimensity 1080, 8 ГБ ОЗУ, поддержка стилуса.",
                price = 27990.0,
                imageUrl = "https://picsum.photos/seed/tablet1/400/400",
                category = "Электроника",
                rating = 4.4f
            ),
            ProductEntity(
                id = 12,
                name = "Портативная колонка BoomBox Mini",
                description = "360° звук, IPX7, 24 ч работы, мощность 20 Вт, встроенный микрофон.",
                price = 4990.0,
                imageUrl = "https://picsum.photos/seed/speaker1/400/400",
                category = "Электроника",
                rating = 4.5f
            )
        )
    }
}
