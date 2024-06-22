create VIEW ChefsView AS
SELECT 
Chefs.ChefId,
concat(ChefFirstName,' ',ChefLastName) AS Name,
Chefs.Active,
DATEDIFF(year, birthdate, GETDATE()) - 
CASE 
WHEN GETDATE() < DATEADD(year, DATEDIFF(year, birthdate, GETDATE()), birthdate) THEN 1 
ELSE 0 
END AS age,
Chefs.City,
Chefs.Governorate,
Chefs.Rating,
Chefs.RestaurantName,
Chefs.Role,
Chefs.birthdate
FROM Chefs

SELECT * FROM ChefsView