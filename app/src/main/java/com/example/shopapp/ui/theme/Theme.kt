package com.example.shopapp.ui.theme

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable

private val LightColors = lightColorScheme(
    primary            = BrandBlue,
    onPrimary          = SurfaceWhite,
    primaryContainer   = LightBlue,
    onPrimaryContainer = BrandBlueDark,
    secondary          = AccentAmber,
    onSecondary        = TextPrimary,
    secondaryContainer = LightBlue,
    onSecondaryContainer = TextPrimary,
    background         = BackgroundGray,
    onBackground       = TextPrimary,
    surface            = SurfaceWhite,
    onSurface          = TextPrimary,
    surfaceVariant     = BackgroundGray,
    onSurfaceVariant   = TextSecondary,
    error              = ErrorRed,
    outline            = DividerColor
)

@Composable
fun ShopTheme(content: @Composable () -> Unit) {
    MaterialTheme(
        colorScheme = LightColors,
        typography  = Typography,
        content     = content
    )
}
