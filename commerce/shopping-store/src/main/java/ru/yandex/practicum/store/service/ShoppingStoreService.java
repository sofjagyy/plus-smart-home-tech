package ru.yandex.practicum.store.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.interaction.api.dto.ProductDto;
import ru.yandex.practicum.interaction.api.dto.SetProductQuantityStateRequest;
import ru.yandex.practicum.interaction.api.enums.ProductCategory;
import ru.yandex.practicum.interaction.api.enums.ProductState;
import ru.yandex.practicum.interaction.api.exception.ProductNotFoundException;
import ru.yandex.practicum.store.entity.Product;
import ru.yandex.practicum.store.repository.ProductRepository;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ShoppingStoreService {

    private final ProductRepository productRepository;

    @Transactional(readOnly = true)
    public Page<ProductDto> getProducts(ProductCategory category, Pageable pageable) {
        return productRepository
                .findByProductCategoryAndProductState(category, ProductState.ACTIVE, pageable)
                .map(this::toDto);
    }

    @Transactional(readOnly = true)
    public ProductDto getProduct(UUID productId) {
        return toDto(findProduct(productId));
    }

    @Transactional
    public ProductDto createProduct(ProductDto dto) {
        Product product = Product.builder()
                .productName(dto.getProductName())
                .description(dto.getDescription())
                .imageSrc(dto.getImageSrc())
                .quantityState(dto.getQuantityState())
                .productState(dto.getProductState())
                .productCategory(dto.getProductCategory())
                .price(dto.getPrice())
                .build();
        return toDto(productRepository.save(product));
    }

    @Transactional
    public ProductDto updateProduct(ProductDto dto) {
        Product product = findProduct(dto.getProductId());
        product.setProductName(dto.getProductName());
        product.setDescription(dto.getDescription());
        product.setImageSrc(dto.getImageSrc());
        product.setQuantityState(dto.getQuantityState());
        product.setProductState(dto.getProductState());
        product.setProductCategory(dto.getProductCategory());
        product.setPrice(dto.getPrice());
        return toDto(productRepository.save(product));
    }

    @Transactional
    public boolean removeProduct(UUID productId) {
        Product product = findProduct(productId);
        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        return true;
    }

    @Transactional
    public boolean setQuantityState(SetProductQuantityStateRequest request) {
        Product product = findProduct(request.getProductId());
        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);
        return true;
    }

    private Product findProduct(UUID productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found: " + productId));
    }

    private ProductDto toDto(Product product) {
        return ProductDto.builder()
                .productId(product.getProductId())
                .productName(product.getProductName())
                .description(product.getDescription())
                .imageSrc(product.getImageSrc())
                .quantityState(product.getQuantityState())
                .productState(product.getProductState())
                .productCategory(product.getProductCategory())
                .price(product.getPrice())
                .build();
    }
}
