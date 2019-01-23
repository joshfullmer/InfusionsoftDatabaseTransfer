Notes:
 
Spoke with Nathan via phone.
 
This is what they need transferred:
 
"JJumpp 220" Tag Id 323 in yb140: this is the tag applied to the contacts that need transferred
standard fields
custom fields
don't care about tasks/appts
tags
order history
subscriptions
 
Things Nathan mentioned:
-          He said that they have some old subscriptions in yb140 that they no longer sell but they need to transfer them over as theyíre grandfathered in for contacts that currently have them
 
Nathan said that theyíre fine waiting to transfer until next week.
 
Nathan has created some of the custom fields already in yf338. Heís going to provide a ìmappingî of the custom fields from yb140:yf338



/*
2016-12-19

Have done this so far
*/

#Transferred Contact and ContactGroupAssign

CREATE TABLE yb140_to_yf338_ContactIds_20161219 AS (
SELECT t1.Id FROM `Contact_yb140_20161219` t1
INNER JOIN `ContactGroupAssign_yb140_20161219` t2 ON t2.ContactId=t1.Id AND t2.GroupId=323
GROUP BY t1.Id);

ALTER TABLE yb140_to_yf338_ContactIds_20161219 ADD INDEX Id(Id);

#User record match up:

SELECT t1.Id,t2.Id FROM Contact t1
INNER JOIN Contact_yb140_20161219 t2 ON t2.FirstName=t1.FirstName AND t2.LastName=t1.LastName AND t2.Email=t1.Email AND t2.IsUser>0
WHERE t1.IsUser>0;

#Created transfer table with column `yb140Id`, the yf338.Contact table had auto increment value of 846 so I set the table yb140_to_yf338_ContactTransferTable_20161219 auto increment to be 1046.
#CREATE TABLE `yb140_to_yf338_ContactTransferTable_20161219`;

#Contacts transferred over to the "transfer table", if their auto_increment increases just update Id to add some values.

INSERT INTO yb140_to_yf338_ContactTransferTable_20161219 (yb140Id, FirstName, LastName, Company, Phone1Type, Phone1, Phone1Ext, Phone2Type, Phone2, Phone2Ext, Phone3Type, Phone3, Phone3Ext, Email, StreetAddress1, OffSetTimeZone, StreetAddress2, City, State, PostalCode, ZipFour1, Country, ContactNotes, Username, PASSWORD, Website, Title, CompanyID, ReferralCode, Validated, Groups, MiddleName, Suffix, Nickname, JobTitle, Address1Type, Address2Street1, Address2Street2, City2, State2, PostalCode2, ZipFour2, Country2, Address2Type, Birthday, EmailAddress2, SpouseName, EmailAddress3, Address3Type, Address3Street1, Address3Street2, City3, State3, PostalCode3, ZipFour3, Country3, AssistantName, AssistantPhone, Phone4Type, Phone4, Phone4Ext, Phone5Type, Phone5, Phone5Ext, BillingInformation, Fax1Type, Fax1, Fax2, Fax2Type, Anniversary, ContactType, DateCreated, CreatedBy, LastUpdated, LastUpdatedBy, IsUser, SSN, OwnerID, LeadSourceId, LastUpdatedUtcMillis, BrowserLanguage, TimeZone, LanguageTag)
SELECT t1.Id, t1.FirstName, t1.LastName, t1.Company, t1.Phone1Type, t1.Phone1, t1.Phone1Ext, t1.Phone2Type, t1.Phone2, t1.Phone2Ext, t1.Phone3Type, t1.Phone3, t1.Phone3Ext, t1.Email, t1.StreetAddress1, t1.OffSetTimeZone, t1.StreetAddress2, t1.City, t1.State, t1.PostalCode, t1.ZipFour1, t1.Country, t1.ContactNotes, t1.Username, t1.Password, t1.Website, t1.Title, t1.CompanyID, t1.ReferralCode, t1.Validated, t1.Groups, t1.MiddleName, t1.Suffix, t1.Nickname, t1.JobTitle, t1.Address1Type, t1.Address2Street1, t1.Address2Street2, t1.City2, t1.State2, t1.PostalCode2, t1.ZipFour2, t1.Country2, t1.Address2Type, t1.Birthday, t1.EmailAddress2, t1.SpouseName, t1.EmailAddress3, t1.Address3Type, t1.Address3Street1, t1.Address3Street2, t1.City3, t1.State3, t1.PostalCode3, t1.ZipFour3, t1.Country3, t1.AssistantName, t1.AssistantPhone, t1.Phone4Type, t1.Phone4, t1.Phone4Ext, t1.Phone5Type, t1.Phone5, t1.Phone5Ext, t1.BillingInformation, t1.Fax1Type, t1.Fax1, t1.Fax2, t1.Fax2Type, t1.Anniversary, t1.ContactType, t1.DateCreated, t1.CreatedBy, t1.LastUpdated, t1.LastUpdatedBy, t1.IsUser, t1.SSN, t1.OwnerID, t1.LeadSourceId, t1.LastUpdatedUtcMillis, t1.BrowserLanguage, t1.TimeZone, t1.LanguageTag
FROM `Contact_yb140_20161219` t1
INNER JOIN yb140_to_yf338_ContactIds_20161219 t2 ON t2.Id=t1.Id;

SELECT * FROM yb140_to_yf338_ContactTransferTable_20161219 t1
INNER JOIN yb140_to_yf338_ContactIds_20161219 t2 ON t2.Id=t1.yb140Id;


#Created transfer table for ContactGroup with column `yb140Id` the yf339.ContactGroup had auto inc of 200 so setting this to be 250
#CREATE TABLE `yb140_to_yf339_ContactGroupTransferTable_20161219`;

CREATE TABLE `yb140_to_yf339_ContactGroupTransferTable_20161219` (
  `Id` INT(10) NOT NULL AUTO_INCREMENT,
  `yb140Id` INT(10),
  `GroupName` VARCHAR(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `GroupCategoryId` INT(10) DEFAULT NULL,
  `GroupDescription` MEDIUMTEXT COLLATE utf8_unicode_ci,
  `ImportId` INT(11) DEFAULT '0',
  `ContentPublishingId` CHAR(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `ContentPublishingId_UNIQUE` (`ContentPublishingId`),
  KEY `GroupCategoryId` (`GroupCategoryId`),
  KEY `GroupName` (`GroupName`),
  KEY `yb140Id` (`yb140Id`)
) ENGINE=INNODB AUTO_INCREMENT=250 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO yb140_to_yf339_ContactGroupTransferTable_20161219 (yb140Id,GroupName,GroupCategoryId,GroupDescription)
SELECT Id,GroupName,GroupCategoryId,GroupDescription FROM `ContactGroup_yb140_20161219`;

#More or less did the same thing for ContactGroupCategory yf338 auto was 46, set it to be 56

CREATE TABLE `yb140_to_yf339_ContactGroupCategoryTransferTable_20161219` (
  `Id` INT(10) NOT NULL AUTO_INCREMENT,
  `yb140Id` INT(10),
  `CategoryName` VARCHAR(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `CategoryDescription` MEDIUMTEXT COLLATE utf8_unicode_ci,
  `ImportId` INT(11) DEFAULT '0',
  `ContentPublishingId` CHAR(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `ContentPublishingId_UNIQUE` (`ContentPublishingId`),
  KEY `CategoryName` (`CategoryName`)
) ENGINE=INNODB AUTO_INCREMENT=56 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO yb140_to_yf339_ContactGroupCategoryTransferTable_20161219 (yb140Id,CategoryName,CategoryDescription)
SELECT Id,CategoryName,CategoryDescription FROM `ContactGroupCategory_yb140_20161219`;

#Then updated the ContactGroup transfer category Id:

UPDATE yb140_to_yf339_ContactGroupTransferTable_20161219 t1
INNER JOIN yb140_to_yf339_ContactGroupCategoryTransferTable_20161219 t2 ON t2.yb140Id=t1.GroupCategoryId
SET t1.GroupCategoryId=t2.Id;

#To transfer tag applications, won't need to create a new table, just do this:

/*
INSERT INTO ContactGroupAssign (GroupId,ContactId,DateCreated)
SELECT t2.Id,t3.yf338Id,t1.DateCreated FROM `ContactGroupAssign_yb140_20161219` t1
INNER JOIN yb140_to_yf339_ContactGroupTransferTable_20161219 t2 ON t2.yb140Id=t1.GroupId
INNER JOIN yb140_to_yf338_ContactIds_20161219 t3 ON t3.yb140Id=t1.ContactId;

#Then sync tags
*/

/*
Now to complete transfer for Contact ContactGroup ContactGroupCategory then insert data for ContactGroupAssign (and sync)
*/

#Contact:

INSERT INTO Contact (Id, FirstName, LastName, Company, Phone1Type, Phone1, Phone1Ext, Phone2Type, Phone2, Phone2Ext, Phone3Type, Phone3, Phone3Ext, Email, StreetAddress1, OffSetTimeZone, StreetAddress2, City, State, PostalCode, ZipFour1, Country, ContactNotes, Username, PASSWORD, Website, Title, CompanyID, ReferralCode, Validated, Groups, MiddleName, Suffix, Nickname, JobTitle, Address1Type, Address2Street1, Address2Street2, City2, State2, PostalCode2, ZipFour2, Country2, Address2Type, Birthday, EmailAddress2, SpouseName, EmailAddress3, Address3Type, Address3Street1, Address3Street2, City3, State3, PostalCode3, ZipFour3, Country3, AssistantName, AssistantPhone, Phone4Type, Phone4, Phone4Ext, Phone5Type, Phone5, Phone5Ext, BillingInformation, Fax1Type, Fax1, Fax2, Fax2Type, Anniversary, ContactType, DateCreated, CreatedBy, LastUpdated, LastUpdatedBy, IsUser, SSN, OwnerID, LeadSourceId, LastUpdatedUtcMillis, BrowserLanguage, TimeZone, LanguageTag)
SELECT Id, FirstName, LastName, Company, Phone1Type, Phone1, Phone1Ext, Phone2Type, Phone2, Phone2Ext, Phone3Type, Phone3, Phone3Ext, Email, StreetAddress1, OffSetTimeZone, StreetAddress2, City, State, PostalCode, ZipFour1, Country, ContactNotes, Username, PASSWORD, Website, Title, CompanyID, ReferralCode, Validated, Groups, MiddleName, Suffix, Nickname, JobTitle, Address1Type, Address2Street1, Address2Street2, City2, State2, PostalCode2, ZipFour2, Country2, Address2Type, Birthday, EmailAddress2, SpouseName, EmailAddress3, Address3Type, Address3Street1, Address3Street2, City3, State3, PostalCode3, ZipFour3, Country3, AssistantName, AssistantPhone, Phone4Type, Phone4, Phone4Ext, Phone5Type, Phone5, Phone5Ext, BillingInformation, Fax1Type, Fax1, Fax2, Fax2Type, Anniversary, ContactType, DateCreated, CreatedBy, LastUpdated, LastUpdatedBy, IsUser, SSN, OwnerID, LeadSourceId, LastUpdatedUtcMillis, BrowserLanguage, TimeZone, LanguageTag
FROM yb140_to_yf338_ContactTransferTable_20161219;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.OwnerID=30
WHERE t2.OwnerID=13871;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.OwnerID=30
WHERE t2.OwnerID=13871;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.OwnerID=1
WHERE t2.OwnerID>0;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.CreatedBy=30
WHERE t2.CreatedBy=13871;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.CreatedBy=1
WHERE t2.CreatedBy>0;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.LastUpdatedBy=30
WHERE t2.LastUpdatedBy=13871;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.LastUpdatedBy=1
WHERE t2.LastUpdatedBy>0;

UPDATE `yb140_to_yf338_ContactIds_20161219` t1
INNER JOIN Contact t2 ON t2.Id=t1.yf338Id
SET t2.LeadSourceId=0;

#ContactGroup stuff:

INSERT INTO ContactGroupCategory (Id, CategoryName, CategoryDescription, ImportId, ContentPublishingId)
SELECT Id, CategoryName, CategoryDescription, ImportId, ContentPublishingId
FROM yb140_to_yf339_ContactGroupCategoryTransferTable_20161219;

INSERT INTO ContactGroup (Id, GroupName, GroupCategoryId, GroupDescription, ImportId, ContentPublishingId)
SELECT Id, GroupName, GroupCategoryId, GroupDescription, ImportId, ContentPublishingId
FROM yb140_to_yf339_ContactGroupTransferTable_20161219;

INSERT INTO ContactGroupAssign (GroupId,ContactId,DateCreated)
SELECT t2.Id,t3.yf338Id,t1.DateCreated FROM `ContactGroupAssign_yb140_20161219` t1
INNER JOIN yb140_to_yf339_ContactGroupTransferTable_20161219 t2 ON t2.yb140Id=t1.GroupId
INNER JOIN yb140_to_yf338_ContactIds_20161219 t3 ON t3.yb140Id=t1.ContactId;

#Then synced tags

/*
Transferring Custom_Contact:
*/

INSERT INTO Custom_Contact (Id)
SELECT yf338Id FROM `yb140_to_yf338_ContactIds_20161219`;

UPDATE Custom_Contact t1
INNER JOIN yb140_to_yf338_ContactIds_20161219 t2 ON t2.yf338Id=t1.Id
INNER JOIN `Custom_Contact_yb140_20161219` t3 ON t3.Id=t2.yb140Id
SET t1.ReactivationNotes=t3.ReactivationNotes,
t1.SaleType=t3.SaleType,
t1.VerificationPIN=t3.VerificationPIN,
t1.PDOrgID=t3.PDOrgID,
t1.AccountManager=t3.AccountManager,
t1.AdditionalNotes=t3.AdditionalNotes,
t1.Agent0=t3.Agent0,
t1.BusinessDescription=t3.BusinessDescription,
t1.BusinessHours=t3.BusinessHours,
t1.Company0=t3.Company0,
t1.GoogleListing=t3.GoogleListing,
t1.GoogleVerificationPIN=t3.GoogleVerificationPIN,
t1.Industry=t3.Industry,
t1.Keywords=t3.Keywords,
t1.MultipleLocations=t3.MultipleLocations,
t1.Product1=t3.Product1,
t1.ReactivationAgent=t3.RetentionManager,
t1.UpsaleAgent=t3.SuccessManager,
t1.UpsellNotes=t3.UpsellNotes;

/*
Transfer creditcard
*/

#created table yb140_to_yf338_CreditCardTransferTable_20161219 with a yb140Id column. yf338 auto was 382, set the transfer table to 402

INSERT INTO yb140_to_yf338_CreditCardTransferTable_20161219 (yb140Id, ContactId, BillName, PhoneNumber, Email, BillAddress1, BillAddress2, BillCity, BillState, BillZip, BillCountry, ShipAddress1, ShipAddress2, ShipCity, ShipState, ShipZip, ShipCountry, ShipName, ShipFirstName, ShipLastName, NameOnCard, FirstName, LastName, CardType, CardNumber, ExpirationMonth, StartDateMonth, StartDateYear, ExpirationYear, `Status`, StatusMsg, ShipCompanyName, ShipPhoneNumber, MiddleName, CompanyName, ShipMiddleName, Last4, MaestroIssueNumber, VaultId, CapturedVerificationCode, ExternallyUpdated)
SELECT Id, ContactId, BillName, PhoneNumber, Email, BillAddress1, BillAddress2, BillCity, BillState, BillZip, BillCountry, ShipAddress1, ShipAddress2, ShipCity, ShipState, ShipZip, ShipCountry, ShipName, ShipFirstName, ShipLastName, NameOnCard, FirstName, LastName, CardType, CardNumber, ExpirationMonth, StartDateMonth, StartDateYear, ExpirationYear, `Status`, StatusMsg, ShipCompanyName, ShipPhoneNumber, MiddleName, CompanyName, ShipMiddleName, Last4, MaestroIssueNumber, VaultId, CapturedVerificationCode, ExternallyUpdated
FROM From_yb140_20161214;

UPDATE yb140_to_yf338_CreditCardTransferTable_20161219 t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
SET t1.ContactId=t2.yf338Id;

INSERT INTO CreditCard (Id, ContactId, BillName, PhoneNumber, Email, BillAddress1, BillAddress2, BillCity, BillState, BillZip, BillCountry, ShipAddress1, ShipAddress2, ShipCity, ShipState, ShipZip, ShipCountry, ShipName, ShipFirstName, ShipLastName, NameOnCard, FirstName, LastName, CardType, CardNumber, ExpirationMonth, StartDateMonth, StartDateYear, ExpirationYear, `Status`, StatusMsg, ShipCompanyName, ShipPhoneNumber, MiddleName, CompanyName, ShipMiddleName, Last4, MaestroIssueNumber, VaultId, CapturedVerificationCode, ExternallyUpdated)
SELECT Id, ContactId, BillName, PhoneNumber, Email, BillAddress1, BillAddress2, BillCity, BillState, BillZip, BillCountry, ShipAddress1, ShipAddress2, ShipCity, ShipState, ShipZip, ShipCountry, ShipName, ShipFirstName, ShipLastName, NameOnCard, FirstName, LastName, CardType, CardNumber, ExpirationMonth, StartDateMonth, StartDateYear, ExpirationYear, `Status`, StatusMsg, ShipCompanyName, ShipPhoneNumber, MiddleName, CompanyName, ShipMiddleName, Last4, MaestroIssueNumber, VaultId, CapturedVerificationCode, ExternallyUpdated
FROM yb140_to_yf338_CreditCardTransferTable_20161219;



/*
#First Section:

Next part is transferring Jobs (Job,Invoice,etc)

First I created the "transfer" tables yb140_to_yf338_table_20161222

Second (next section) will be to update key ids

Third (last section) will be to add the data to the live apps
*/

#Product:
#created table yb140_to_yf338_Product_20161222 with yb140Id column, yf338 auto was 118, set transfer to be 138

#Done:
#INSERT INTO yb140_to_yf338_Product_20161222 (yb140Id, ProductName, ProductShortDesc, ProductPrice, ProductDesc, ProductCost, Status, SubCategory, ProductDate, PercentSavings, ProductImagePath, Favorite, VendorURL, Type, Family, ProdMfrID, SizeTypeID, Weight, Sku, IsPackage, LeadAmt, LeadPercent, SaleAmt, SalePercent, PayoutType, PurchasedContactTemplate, ProductImageLarge, ProductImageSmall, IsHidden, ShippingTime, InventoryLimit, InventoryNotifiee, InventoryEmailSent, BottomHTML, TopHTML, Taxable, IsDigital, DigitalTemplateId, DownloadHeader, DownloadFooter, DownloadLength, DownloadLimit, Shippable, CountryTaxable, StateTaxable, CityTaxable, LegacyCProgramId, SubscriptionOnly, DateCreated, LastUpdated, OutOfStockEnabled)
SELECT Id, ProductName, ProductShortDesc, ProductPrice, ProductDesc, ProductCost, STATUS, SubCategory, ProductDate, PercentSavings, ProductImagePath, Favorite, VendorURL, TYPE, Family, ProdMfrID, SizeTypeID, Weight, Sku, IsPackage, LeadAmt, LeadPercent, SaleAmt, SalePercent, PayoutType, PurchasedContactTemplate, ProductImageLarge, ProductImageSmall, IsHidden, ShippingTime, InventoryLimit, InventoryNotifiee, InventoryEmailSent, BottomHTML, TopHTML, Taxable, IsDigital, DigitalTemplateId, DownloadHeader, DownloadFooter, DownloadLength, DownloadLimit, Shippable, CountryTaxable, StateTaxable, CityTaxable, LegacyCProgramId, SubscriptionOnly, DateCreated, LastUpdated, OutOfStockEnabled
FROM `Product_yb140_20161222`;

#Invoice:
#created table yb140_to_yf338_Invoice_20161222 with yb140Id column, yf338 auto was 550, set transfer to be 650

#Done:
#INSERT INTO yb140_to_yf338_Invoice_20161222 (yb140Id, ContactId, UserCreate, DateCreated, InvoiceTotal, DateSent, JobId, DueDate, InvoicePath, PayPlanId, TotalDue, AffiliateId, CreditStatus, JobClass, PayStatus, Ad, Description, TotalPaid, ProductSold, PayPlanStatus, RefundStatus, InvoiceType, PromoCode, LeadAffiliateId, AllowDup, Synced, Active, MarketingEmailId, LastUpdated, XID, AllowPayment)
SELECT t1.Id, t1.ContactId, t1.UserCreate, t1.DateCreated, t1.InvoiceTotal, t1.DateSent, t1.JobId, t1.DueDate, t1.InvoicePath, t1.PayPlanId, t1.TotalDue, t1.AffiliateId, t1.CreditStatus, t1.JobClass, t1.PayStatus, t1.Ad, t1.Description, t1.TotalPaid, t1.ProductSold, t1.PayPlanStatus, t1.RefundStatus, t1.InvoiceType, t1.PromoCode, t1.LeadAffiliateId, t1.AllowDup, t1.Synced, t1.Active, t1.MarketingEmailId, t1.LastUpdated, t1.XID, t1.AllowPayment
FROM `Invoice_yb140_20161222` t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
GROUP BY t1.Id;


#InvoiceItem:
#created table yb140_to_yf338_InvoiceItem_20161219 with yb140Id column, yf338 auto was 1458, set transfer to be 1858

#Done:
#INSERT INTO yb140_to_yf338_InvoiceItem_20161219 (yb140Id, InvoiceId, JobId, InvoiceAmt, UserCreate, DateCreated, Note, Description, JobClass, Discount, InvoiceGroup, InvoiceCode, ContactId, ChargeClass, ChargeIds, CommissionStatus, OrderItemId, ApplyDiscountToCommission, LastUpdated)
SELECT t1.Id, t1.InvoiceId, t1.JobId, t1.InvoiceAmt, t1.UserCreate, t1.DateCreated, t1.Note, t1.Description, t1.JobClass, t1.Discount, t1.InvoiceGroup, t1.InvoiceCode, t1.ContactId, t1.ChargeClass, t1.ChargeIds, t1.CommissionStatus, t1.OrderItemId, t1.ApplyDiscountToCommission, t1.LastUpdated
FROM `InvoiceItem_yb140_20161222` t1
INNER JOIN yb140_to_yf338_Invoice_20161222 t2 ON t2.yb140Id=t1.InvoiceId;


#InvoicePayment:
#created table yb140_to_yf338_InvoicePayment_20161222 with yb140Id column, yf338 auto was 446, set transfer to be 646

#Done:
#INSERT INTO yb140_to_yf338_InvoicePayment_20161222 (yb140Id, InvoiceId, PaymentId, Amt, Note, PayDate, PayStatus, SkipCommission, LastUpdated, RefundInvoicePaymentId)
SELECT t1.Id, t1.InvoiceId, t1.PaymentId, t1.Amt, t1.Note, t1.PayDate, t1.PayStatus, t1.SkipCommission, t1.LastUpdated, t1.RefundInvoicePaymentId
FROM `InvoicePayment_yb140_20161222` t1
INNER JOIN yb140_to_yf338_Invoice_20161222 t2 ON t2.yb140Id=t1.InvoiceId;


#Job:
#created table yb140_to_yf338_Job_20161222 with yb140Id column, yf338 auto was 550, set transfer to be 650

#Done:
#INSERT INTO yb140_to_yf338_Job_20161222 (yb140Id, JobTitle, OppId, ContactId, StartDate, DueDate, TechId, JobNotes, SalesId, FeatureCats, ProjectPrice, DefaultHourlyRate, JobType, RecurType, PercentComplete, ProductId, JobStatus, InvoiceHourLogs, DateCreated, CreatedBy, LastUpdated, LastUpdatedBy, JobHandlerClass, InvoiceId, AffiliateId, PromoCode, OrderType, OrderStatus, JumpLogId, LeadAffiliateId, EmailInvoiceFlag, HaveSuccessActionsRun, ShippingAddressId, MarketingEmailId, JobRecurringId, LegacyJobRecurringInstanceId, FileBoxId)
SELECT t1.Id, t1.JobTitle, t1.OppId, t1.ContactId, t1.StartDate, t1.DueDate, t1.TechId, t1.JobNotes, t1.SalesId, t1.FeatureCats, t1.ProjectPrice, t1.DefaultHourlyRate, t1.JobType, t1.RecurType, t1.PercentComplete, t1.ProductId, t1.JobStatus, t1.InvoiceHourLogs, t1.DateCreated, t1.CreatedBy, t1.LastUpdated, t1.LastUpdatedBy, t1.JobHandlerClass, t1.InvoiceId, t1.AffiliateId, t1.PromoCode, t1.OrderType, t1.OrderStatus, t1.JumpLogId, t1.LeadAffiliateId, t1.EmailInvoiceFlag, t1.HaveSuccessActionsRun, t1.ShippingAddressId, t1.MarketingEmailId, t1.JobRecurringId, t1.LegacyJobRecurringInstanceId, t1.FileBoxId
FROM `Job_yb140_20161222` t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
GROUP BY t1.Id;


#OrderItem:
#created table yb140_to_yf338_OrderItem_20161222 with yb140Id column, yf338 auto was 1470, set transfer to be 1870

#Done:
#INSERT INTO yb140_to_yf338_OrderItem_20161222 (yb140Id, OrderId, ProductId, DiscountedOrderItemId, ItemName, Qty, CPU, PPU, ItemDescription, Notes, Serial, ServiceEnd, ShipDate, RecdDate, TrackingNumber, Carrier, ETA, ChargeInventory, InvoiceItemId, ItemType, SpecialId, Options, Locked, ApplyDiscountToCommission, SubscriptionPlanId, SourceOrderItemId, LegacyJobRecurringInstanceId, LegacyJobRecurringInstanceItemId, SubscriptionPlanDesc, LastUpdated)
SELECT t1.Id, t1.OrderId, t1.ProductId, t1.DiscountedOrderItemId, t1.ItemName, t1.Qty, t1.CPU, t1.PPU, t1.ItemDescription, t1.Notes, t1.Serial, t1.ServiceEnd, t1.ShipDate, t1.RecdDate, t1.TrackingNumber, t1.Carrier, t1.ETA, t1.ChargeInventory, t1.InvoiceItemId, t1.ItemType, t1.SpecialId, t1.Options, t1.Locked, t1.ApplyDiscountToCommission, t1.SubscriptionPlanId, t1.SourceOrderItemId, t1.LegacyJobRecurringInstanceId, t1.LegacyJobRecurringInstanceItemId, t1.SubscriptionPlanDesc, t1.LastUpdated
FROM `OrderItem_yb140_20161222` t1
INNER JOIN yb140_to_yf338_Job_20161222 t2 ON t2.yb140Id=t1.OrderId
GROUP BY t1.Id;


#Payment:
#created table yb140_to_yf338_Payment_20161222 with yb140Id column, yf338 auto was 450, set transfer to be 650

#Done:
#INSERT INTO yb140_to_yf338_Payment_20161222 (yb140Id, PayDate, UserId, PayAmt, PayType, ContactId, PayNote, InvoiceId, RefundId, ChargeId, Commission, Synced, LastUpdated, TransactionId, CollectionMethod, PaymentSubType, PaymentGatewayId, State)
SELECT t1.Id, t1.PayDate, t1.UserId, t1.PayAmt, t1.PayType, t1.ContactId, t1.PayNote, t1.InvoiceId, t1.RefundId, t1.ChargeId, t1.Commission, t1.Synced, t1.LastUpdated, t1.TransactionId, t1.CollectionMethod, t1.PaymentSubType, t1.PaymentGatewayId, t1.State
FROM `Payment_yb140_20161222` t1
INNER JOIN yb140_to_yf338_InvoicePayment_20161222 t2 ON t2.PaymentId=t1.Id
GROUP BY t1.Id;


#PayPlan:
#created table yb140_to_yf338_PayPlan_20161222 with yb140Id column, yf338 auto was 556, set transfer to be 656

#Done:
#INSERT INTO yb140_to_yf338_PayPlan_20161222 (yb140Id, InvoiceId, DateDue, AmtDue, AutoCharge, CC1, CC2, NumDaysBetweenRetry, MaxRetry, SendInvoices, SendInvoiceNumDays, MerchantAccountId, Type, NumDaysBetween, InitDate, StartDate, FirstPayAmt, NumPmts, PaymentGatewayId, PaymentSubType, PayPalRefTxnId)
SELECT t1.Id, t1.InvoiceId, t1.DateDue, t1.AmtDue, t1.AutoCharge, t1.CC1, t1.CC2, t1.NumDaysBetweenRetry, t1.MaxRetry, t1.SendInvoices, t1.SendInvoiceNumDays, t1.MerchantAccountId, t1.Type, t1.NumDaysBetween, t1.InitDate, t1.StartDate, t1.FirstPayAmt, t1.NumPmts, t1.PaymentGatewayId, t1.PaymentSubType, t1.PayPalRefTxnId
FROM `PayPlan_yb140_20161222` t1
INNER JOIN yb140_to_yf338_Invoice_20161222 t2 ON t2.yb140Id=t1.InvoiceId
GROUP BY t1.Id;


#PayPlanItem:
#created table yb140_to_yf338_PayPlanItem_20161222 with yb140Id column, yf338 auto was 562, set transfer to be 962

#Done:
#INSERT INTO yb140_to_yf338_PayPlanItem_20161222 (yb140Id, PayPlanId, DateDue, AmtDue, Status, AmtPaid)
SELECT t1.Id, t1.PayPlanId, t1.DateDue, t1.AmtDue, t1.Status, t1.AmtPaid
FROM `PayPlanItem_yb140_20161222` t1
INNER JOIN yb140_to_yf338_PayPlan_20161222 t2 ON t2.yb140Id=t1.PayPlanId
GROUP BY t1.Id;



/*
#Second Section:

Next part is transferring Jobs (Job,Invoice,etc)

First I created the "transfer" tables yb140_to_yf338_table_20161222

Second (next section) will be to update key ids

Third (last section) will be to add the data to the live apps
*/

/*
Need to do: yb140_to_yf338_Invoice_20161222
*/

#UPDATE yb140_to_yf338_Invoice_20161222 t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
SET t1.ContactId=t2.yf338Id;

#UPDATE yb140_to_yf338_Invoice_20161222 t1
SET t1.AffiliateId=0,
t1.LeadAffiliateId=0;

#UPDATE yb140_to_yf338_Invoice_20161222 t1
INNER JOIN yb140_to_yf338_Job_20161222 t2 ON t2.yb140Id=t1.JobId
SET t1.JobId=t2.Id;

#UPDATE yb140_to_yf338_Invoice_20161222 t1
INNER JOIN yb140_to_yf338_PayPlan_20161222 t2 ON t2.yb140Id=t1.PayPlanId
SET t1.PayPlanId=t2.Id;


/*
Need to do: yb140_to_yf338_InvoiceItem_20161219
*/

#UPDATE yb140_to_yf338_InvoiceItem_20161219 t1
INNER JOIN `yb140_to_yf338_Invoice_20161222` t2 ON t2.yb140Id=t1.InvoiceId
SET t1.Invoice=t2.Id;

#UPDATE yb140_to_yf338_InvoiceItem_20161219 t1
INNER JOIN yb140_to_yf338_OrderItem_20161222 t2 ON t2.yb140Id=t1.OrderItemId
SET t1.OrderItemId=t2.Id;

#UPDATE yb140_to_yf338_InvoiceItem_20161219 t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
SET t1.ContactId=t2.yf338Id;

#UPDATE yb140_to_yf338_InvoiceItem_20161219 t1
INNER JOIN yb140_to_yf338_Job_20161222 t2 ON t2.yb140Id=t1.JobId
SET t1.JobId=t2.Id;

#UPDATE yb140_to_yf338_InvoiceItem_20161219 
SET ChargeIds=NULL;


/*
Need to do: yb140_to_yf338_InvoicePayment_20161222
*/

#UPDATE yb140_to_yf338_InvoicePayment_20161222 t1
INNER JOIN `yb140_to_yf338_Invoice_20161222` t2 ON t2.yb140Id=t1.InvoiceId
SET t1.Invoice=t2.Id;

#UPDATE yb140_to_yf338_InvoicePayment_20161222 t1
INNER JOIN `yb140_to_yf338_InvoicePayment_20161222` t2 ON t2.yb140Id=t1.RefundInvoicePaymentId
SET t1.RefundInvoicePaymentId=t2.Id;

#UPDATE yb140_to_yf338_InvoicePayment_20161222 t1
INNER JOIN `yb140_to_yf338_Payment_20161222` t2 ON t2.yb140Id=t1.PaymentId
SET t1.PaymentId=t2.Id;


/*
Need to do: yb140_to_yf338_Job_20161222
*/

#UPDATE yb140_to_yf338_Job_20161222 t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
SET t1.ContactId=t2.yf338Id;

#UPDATE yb140_to_yf338_Job_20161222 t1
INNER JOIN `yb140_to_yf338_Product_20161222` t2 ON t2.yb140Id=t1.ProductId
SET t1.ProductId=t2.Id;

#UPDATE yb140_to_yf338_Job_20161222 t1
INNER JOIN `yb140_to_yf338_Invoice_20161222` t2 ON t2.yb140Id=t1.InvoiceId
SET t1.InvoiceId=t2.Id;

#UPDATE yb140_to_yf338_Job_20161222
SET CreatedBy=-1,
LastUpdatedBy=-1,
TechId=0,
SalesId=0,
AffiliateId=NULL,
LeadAffiliateId=NULL,
JumpLogId=0,
ShippingAddressId=0,
MarketingEmailId=0,
JobRecurringId=NULL,
LegacyJobRecurringInstanceId=NULL,
FileBoxId=NULL;


/*
Need to do: yb140_to_yf338_OrderItem_20161222
*/

#UPDATE yb140_to_yf338_OrderItem_20161222 t1
INNER JOIN yb140_to_yf338_Job_20161222 t2 ON t2.yb140Id=t1.OrderId
SET t1.OrderId=t2.Id;

#UPDATE yb140_to_yf338_OrderItem_20161222 t1
INNER JOIN yb140_to_yf338_Product_20161222 t2 ON t2.yb140Id=t1.ProductId
SET t1.ProductId=t2.Id;

#UPDATE yb140_to_yf338_OrderItem_20161222 t1
INNER JOIN yb140_to_yf338_InvoiceItem_20161219 t2 ON t2.yb140Id=t1.InvoiceItemId
SET t1.InvoiceItemId=t2.Id;

#UPDATE yb140_to_yf338_OrderItem_20161222
SET SpecialId=0,
SubscriptionPlanId=0,
SourceOrderItemId=0,
DiscountedOrderItemId=0,
LegacyJobRecurringInstanceId=NULL,
LegacyJobRecurringInstanceItemId=NULL;


/*
Need to do: yb140_to_yf338_Payment_20161222
*/

#UPDATE yb140_to_yf338_Payment_20161222 t1
INNER JOIN `yb140_to_yf338_ContactIds_20161219` t2 ON t2.yb140Id=t1.ContactId
SET t1.ContactId=t2.yf338Id;

#UPDATE yb140_to_yf338_Payment_20161222 t1
INNER JOIN `yb140_to_yf338_Invoice_20161222` t2 ON t2.yb140Id=t1.InvoiceId
SET t1.InvoiceId=t2.Id;

#UPDATE yb140_to_yf338_Payment_20161222
SET UserId=0,
RefundId=0,
ChargeId=0,
PaymentGatewayId=0;


/*
Need to do: yb140_to_yf338_PayPlan_20161222
*/

#UPDATE yb140_to_yf338_PayPlan_20161222 t1
INNER JOIN `yb140_to_yf338_Invoice_20161222` t2 ON t2.yb140Id=t1.InvoiceId
SET t1.Invoice=t2.Id;

#UPDATE yb140_to_yf338_PayPlan_20161222 t1
INNER JOIN `yb140_to_yf338_CreditCardTransferTable_20161219` t2 ON t2.yb140Id=t1.CC1
SET t1.CC1=t2.Id;

#UPDATE yb140_to_yf338_PayPlan_20161222
SET CC2=NULL,
MerchantAccountId=0,
PaymentGatewayId=NULL;


/*
Need to do: yb140_to_yf338_PayPlanItem_20161222
*/

#UPDATE yb140_to_yf338_PayPlanItem_20161222 t1
INNER JOIN `yb140_to_yf338_PayPlan_20161222` t2 ON t2.yb140Id=t1.PayPlanId
SET t1.PayPlanId=t2.Id;


/*
#Third Section:

Next part is transferring Jobs (Job,Invoice,etc)

First I created the "transfer" tables yb140_to_yf338_table_20161222

Second (next section) will be to update key ids

Third (last section) will be to add the data to the live apps
*/

SET SESSION group_concat_max_len = 1000000;

SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='Product' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_Product_20161222';

INSERT INTO Product (Id, ProductName, ProductShortDesc, ProductPrice, ProductDesc, ProductCost, STATUS, SubCategory, ProductDate, PercentSavings, ProductImagePath, Favorite, VendorURL, TYPE, Family, ProdMfrID, SizeTypeID, Weight, Sku, IsPackage, LeadAmt, LeadPercent, SaleAmt, SalePercent, PayoutType, PurchasedContactTemplate, ProductImageLarge, ProductImageSmall, IsHidden, ShippingTime, InventoryLimit, InventoryNotifiee, InventoryEmailSent, BottomHTML, TopHTML, Taxable, IsDigital, DigitalTemplateId, DownloadHeader, DownloadFooter, DownloadLength, DownloadLimit, Shippable, CountryTaxable, StateTaxable, CityTaxable, LegacyCProgramId, SubscriptionOnly, DateCreated, LastUpdated, OutOfStockEnabled)
SELECT Id, ProductName, ProductShortDesc, ProductPrice, ProductDesc, ProductCost, STATUS, SubCategory, ProductDate, PercentSavings, ProductImagePath, Favorite, VendorURL, TYPE, Family, ProdMfrID, SizeTypeID, Weight, Sku, IsPackage, LeadAmt, LeadPercent, SaleAmt, SalePercent, PayoutType, PurchasedContactTemplate, ProductImageLarge, ProductImageSmall, IsHidden, ShippingTime, InventoryLimit, InventoryNotifiee, InventoryEmailSent, BottomHTML, TopHTML, Taxable, IsDigital, DigitalTemplateId, DownloadHeader, DownloadFooter, DownloadLength, DownloadLimit, Shippable, CountryTaxable, StateTaxable, CityTaxable, LegacyCProgramId, SubscriptionOnly, DateCreated, LastUpdated, OutOfStockEnabled
FROM yb140_to_yf338_Product_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='Invoice' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_Invoice_20161222';

INSERT INTO Invoice (Id, ContactId, UserCreate, DateCreated, InvoiceTotal, DateSent, JobId, DueDate, InvoicePath, PayPlanId, TotalDue, AffiliateId, CreditStatus, JobClass, PayStatus, Ad, Description, TotalPaid, ProductSold, PayPlanStatus, RefundStatus, InvoiceType, PromoCode, LeadAffiliateId, AllowDup, Synced, Active, MarketingEmailId, LastUpdated, XID, AllowPayment)
SELECT Id, ContactId, UserCreate, DateCreated, InvoiceTotal, DateSent, JobId, DueDate, InvoicePath, PayPlanId, TotalDue, AffiliateId, CreditStatus, JobClass, PayStatus, Ad, Description, TotalPaid, ProductSold, PayPlanStatus, RefundStatus, InvoiceType, PromoCode, LeadAffiliateId, AllowDup, Synced, Active, MarketingEmailId, LastUpdated, XID, AllowPayment
FROM yb140_to_yf338_Invoice_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='InvoiceItem' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_InvoiceItem_20161219';

INSERT INTO InvoiceItem (Id, InvoiceId, JobId, InvoiceAmt, UserCreate, DateCreated, Note, Description, JobClass, Discount, InvoiceGroup, InvoiceCode, ContactId, ChargeClass, ChargeIds, CommissionStatus, OrderItemId, ApplyDiscountToCommission, LastUpdated)
SELECT Id, InvoiceId, JobId, InvoiceAmt, UserCreate, DateCreated, Note, Description, JobClass, Discount, InvoiceGroup, InvoiceCode, ContactId, ChargeClass, ChargeIds, CommissionStatus, OrderItemId, ApplyDiscountToCommission, LastUpdated
FROM yb140_to_yf338_InvoiceItem_20161219;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='InvoicePayment' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_InvoicePayment_20161222';

INSERT INTO InvoicePayment (Id, InvoiceId, PaymentId, Amt, Note, PayDate, PayStatus, SkipCommission, LastUpdated, RefundInvoicePaymentId)
SELECT Id, InvoiceId, PaymentId, Amt, Note, PayDate, PayStatus, SkipCommission, LastUpdated, RefundInvoicePaymentId
FROM yb140_to_yf338_InvoicePayment_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='Job' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_Job_20161222';

INSERT INTO Job (Id, JobTitle, OppId, ContactId, StartDate, DueDate, TechId, JobNotes, SalesId, FeatureCats, ProjectPrice, DefaultHourlyRate, JobType, RecurType, PercentComplete, ProductId, JobStatus, InvoiceHourLogs, DateCreated, CreatedBy, LastUpdated, LastUpdatedBy, JobHandlerClass, InvoiceId, AffiliateId, PromoCode, OrderType, OrderStatus, JumpLogId, LeadAffiliateId, EmailInvoiceFlag, HaveSuccessActionsRun, ShippingAddressId, MarketingEmailId, JobRecurringId, LegacyJobRecurringInstanceId, FileBoxId)
SELECT Id, JobTitle, OppId, ContactId, StartDate, DueDate, TechId, JobNotes, SalesId, FeatureCats, ProjectPrice, DefaultHourlyRate, JobType, RecurType, PercentComplete, ProductId, JobStatus, InvoiceHourLogs, DateCreated, CreatedBy, LastUpdated, LastUpdatedBy, JobHandlerClass, InvoiceId, AffiliateId, PromoCode, OrderType, OrderStatus, JumpLogId, LeadAffiliateId, EmailInvoiceFlag, HaveSuccessActionsRun, ShippingAddressId, MarketingEmailId, JobRecurringId, LegacyJobRecurringInstanceId, FileBoxId
FROM yb140_to_yf338_Job_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='OrderItem' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_OrderItem_20161222';

INSERT INTO OrderItem (Id, OrderId, ProductId, DiscountedOrderItemId, ItemName, Qty, CPU, PPU, ItemDescription, Notes, SERIAL, ServiceEnd, ShipDate, RecdDate, TrackingNumber, Carrier, ETA, ChargeInventory, InvoiceItemId, ItemType, SpecialId, OPTIONS, Locked, ApplyDiscountToCommission, SubscriptionPlanId, SourceOrderItemId, LegacyJobRecurringInstanceId, LegacyJobRecurringInstanceItemId, SubscriptionPlanDesc, LastUpdated)
SELECT Id, OrderId, ProductId, DiscountedOrderItemId, ItemName, Qty, CPU, PPU, ItemDescription, Notes, SERIAL, ServiceEnd, ShipDate, RecdDate, TrackingNumber, Carrier, ETA, ChargeInventory, InvoiceItemId, ItemType, SpecialId, OPTIONS, Locked, ApplyDiscountToCommission, SubscriptionPlanId, SourceOrderItemId, LegacyJobRecurringInstanceId, LegacyJobRecurringInstanceItemId, SubscriptionPlanDesc, LastUpdated
FROM yb140_to_yf338_OrderItem_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='Payment' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_Payment_20161222';

INSERT INTO Payment (Id, PayDate, UserId, PayAmt, PayType, ContactId, PayNote, InvoiceId, RefundId, ChargeId, Commission, Synced, LastUpdated, TransactionId, CollectionMethod, PaymentSubType, PaymentGatewayId, State)
SELECT Id, PayDate, UserId, PayAmt, PayType, ContactId, PayNote, InvoiceId, RefundId, ChargeId, Commission, Synced, LastUpdated, TransactionId, CollectionMethod, PaymentSubType, PaymentGatewayId, State
FROM yb140_to_yf338_Payment_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='PayPlan' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_PayPlan_20161222';

INSERT INTO PayPlan (Id, InvoiceId, DateDue, AmtDue, AutoCharge, CC1, CC2, NumDaysBetweenRetry, MaxRetry, SendInvoices, SendInvoiceNumDays, MerchantAccountId, TYPE, NumDaysBetween, InitDate, StartDate, FirstPayAmt, NumPmts, PaymentGatewayId, PaymentSubType, PayPalRefTxnId)
SELECT Id, InvoiceId, DateDue, AmtDue, AutoCharge, CC1, CC2, NumDaysBetweenRetry, MaxRetry, SendInvoices, SendInvoiceNumDays, MerchantAccountId, TYPE, NumDaysBetween, InitDate, StartDate, FirstPayAmt, NumPmts, PaymentGatewayId, PaymentSubType, PayPalRefTxnId
FROM yb140_to_yf338_PayPlan_20161222;


SELECT GROUP_CONCAT(CONCAT('',t1.COLUMN_NAME) SEPARATOR ', ') FROM information_schema.COLUMNS t1
INNER JOIN information_schema.COLUMNS t2 ON t2.TABLE_NAME='PayPlanItem' AND t2.COLUMN_NAME=t1.COLUMN_NAME
WHERE t1.TABLE_SCHEMA=DATABASE()
AND t1.TABLE_NAME='yb140_to_yf338_PayPlanItem_20161222';

INSERT INTO PayPlanItem (Id, PayPlanId, DateDue, AmtDue, STATUS, AmtPaid)
SELECT Id, PayPlanId, DateDue, AmtDue, STATUS, AmtPaid
FROM yb140_to_yf338_PayPlanItem_20161222;