CREATE VIEW ChefPromoCodeView AS
SELECT 
p.PromoCodeId,
p.Name,
p.DiscountPercentage,
p.StartDate,
p.ExpireDate,
p.MaxLimit,
p.MaxUsers,
p.MinLimit,
c.ChefId
FROM PromoCodes p LEFT join ChefPromoCode c
on p.PromoCodeId = c.PromoCodeId

SELECT * from ChefPromoCodeView