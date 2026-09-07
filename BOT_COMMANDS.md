# 📋 Astratoonix Bot — Poori Command List

Sabhi commands **sirf Admin** (tumhare Telegram account) ke liye kaam karte hain, jab tak "Public" na likha ho.

---

## 🟢 General / Public

| Command | Kya karta hai |
|---|---|
| `/start` | Welcome message — bot ka introduction |
| `/help` | Sabhi commands ki short summary dikhata hai |

---

## 📤 File Upload

| Command | Kya karta hai |
|---|---|
| *(koi bhi file bhejo)* | Direct file bhejne se turant ek permanent streaming link ban jaata hai |
| `/u` | Multi-quality upload session shuru karta hai — iske baad ek-ek karke 480p/720p/1080p files bhejo |
| `/d` | Multi-quality session khatam karta hai — sab qualities ko **ek hi link** mein jod deta hai (Quality switcher ke saath) |

---

## 🔑 Premium & Redeem Codes

| Command | Usage | Kya karta hai |
|---|---|---|
| `/gencode` | `/gencode <days>` | Random redeem code banata hai (jaise 30 din ka) — customer ko bhejo, woh `/redeem` page pe daal ke unlock karega |
| `/deletecode` | `/deletecode <code>` | Ek specific redeem code delete karta hai |
| `/premium` | `/premium <device_id> <days>` | Kisi ek device ko seedha premium de deta hai (code ke bina) |
| `/unpremium` | `/unpremium <device_id>` | Kisi device ka premium hata deta hai |
| `/admin` | `/admin <name> <image_url>` | **Cosmetic:** website pe "Admin" badge ke saath naam+photo dikhata hai (koi real bot-access nahi milta) |
| `/removeadmin` | `/removeadmin <name>` | Cosmetic admin-showcase entry hata deta hai |

---

## 👤 Visitors / Access Control

| Command | Usage | Kya karta hai |
|---|---|---|
| `/user` | — | Recent 30 visitors ki list + unki Access ID dikhata hai |
| `/profile` | `/profile <access_id>` | Ek visitor ki poori profile (naam, device, history) dikhata hai |
| `/approve` | `/approve <access_id>` | Password-protected file ke liye visitor ki request approve karta hai |
| `/block` | `/block <access_id>` | Us visitor ko block kar deta hai (website access band) |
| `/unblock` | `/unblock <access_id>` | Block hataata hai |
| `/reject` | `/reject <access_id>` | Visitor ka poora approval-record delete karta hai (unhe dobara se request karni padegi) |
| `/deleteuser` | `/deleteuser <access_id>` | Visitor ka approval **+ premium dono** delete karta hai — bilkul first-time visitor jaisa reset |
| `/clearpending` | — | Sabhi pending (abhi tak un-approved) visitors ek saath delete karta hai |
| `/ban` | `/ban <user_id>` | Kisi Telegram user ko bot use karne se poori tarah ban karta hai |
| `/unban` | `/unban <user_id>` | Ban hataata hai |

*(Note: Approve/Block/Unblock ke liye Telegram message ke andar inline buttons bhi milte hain — unhe tap karna bhi wahi kaam karta hai jo upar wale commands karte hain)*

---

## 🏷️ File Tagging & Organization

| Command | Usage | Kya karta hai |
|---|---|---|
| `/setpass` | `/setpass <file_id> <password>` ya `off` | File ko password se protect karta hai, ya password hataata hai |
| `/tag` | `/tag <file_id> <Subject>` | File ko Subject/Chapter ke hisaab se category deta hai |
| `/untag` | `/untag <file_id>` | Subject/Chapter tag hataata hai |
| `/setyear` | `/setyear <file_id> <year>` | File ko ek Year (1930-2030) se tag karta hai |
| `/setepisode` | `/setepisode <file_id> <label>` | File ko Season/Episode/Part label deta hai |
| `/expire` | `/expire <file_id> <duration>` | Link ki expiry set karta hai (jaise `7d`, `12h`, `1y`, ya `off` hataane ke liye) |
| `/delete` | `/delete <file_id>` | File poori tarah delete karta hai (agar multi-quality group ka hissa hai, **saari qualities ek saath** delete hoti hain) |
| `/dminem` | — | **SAB** files delete karta hai (confirmation button ke saath — bahut dangerous, sochke use karna) |

---

## 📢 Communication

| Command | Usage | Kya karta hai |
|---|---|---|
| `/reply` | `/reply <message>` | Website pe **sabko** ek glowing RGB banner mein message dikhata hai (jaise cinema announcement) |
| `/clearreply` | — | Announcement banner hataata hai |

---

## 🎨 Site Settings

| Command | Usage | Kya karta hai |
|---|---|---|
| `/advertise` | `/advertise <off\|image\|video>` | Watch page ke top pe ad-banner set/hataata hai |
| `/dashboard` | — | Admin web-dashboard ka secret link deta hai (stats, files, visitor list) |
| `/stats` | — | Quick stats — kitni files, kitne users, kitne views |

---

## 📊 Total Commands: 34

**Categories summary:**
- General: 2
- Upload: 2 (+ file-send)
- Premium/Codes: 6
- Visitor/Access: 10
- File Tagging: 8
- Communication: 2
- Site Settings: 3
