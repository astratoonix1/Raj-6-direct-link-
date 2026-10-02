# 📚 Raj Stream Bot 2.0 — Commands Guide

Total **40 commands**. Zyada tar sirf **Admin** (bot ka owner, `AdminID`) chala sakta hai. Doosre log admin command bhejein to bot ya to chup rehta hai ya "❌ Admin only." bolta hai.

| Kaun chala sakta hai | Commands |
|---|---|
| Sab log | `/start`, `/help` |
| Uploader ya Admin | `/setpass` |
| Sirf Admin | baaki sab (38) |

> `<...>` = zaroori value, `[...]` = optional value. Brackets mat likhna.
> **file_id** = file ki unique ID (link ke aakhir mein jo slug hota hai, jaise `abc123...`). Upload karne par bot ye ID deta hai.
> **access_id** = visitor ki 5-digit ID (jaise `48213`), jo password/approval page pe dikhti hai aur `/user` mein list hoti hai.

---

## 1. Sabke liye

### `/start`
Welcome message dikhata hai. Bot ko koi bhi file bhejo to wo permanent streaming link bana dega.
**Use:** `/start`

### `/help`
Saare commands ki short list dikhata hai.
**Use:** `/help`

---

## 2. File upload aur links

Koi bhi file (video, document, audio, photo) seedha bot ko bhejo, aur bot permanent stream/download/watch link de dega. Iske liye command ki zaroorat nahi.

### `/u` — Multi-quality upload shuru (Admin)
Ek hi movie ki alag-alag quality (480p, 720p, 1080p) ko ek hi link mein jodne ke liye session shuru karta hai.
**Steps:**
1. `/u` bhejo
2. Saari files bhejo. **Filename mein quality likhi honi chahiye**, jaise `Movie.480p.mkv`, `Movie.720p.mkv`, `Movie.1080p.mkv`
3. `/d` bhejo

### `/d` — Multi-quality upload khatam (Admin)
`/u` session band karke ek single link banata hai jisme watch page pe quality switch ka option hota hai. Session nahi chal raha ho to bolta hai "pehle /u bhejo".
**Use:** `/d`

### `/delete` — File delete (Admin)
File ko hata deta hai. Agar file multi-quality group ki hai to saari qualities ek saath delete hoti hain.
**Use:** `/delete <file_id>`
**Example:** `/delete abc123`

### `/expire` — Link ki expiry (Admin)
Link ko kuch time baad band karwata hai. By default sab links permanent hote hain.
**Use:** `/expire <file_id> <time>`
**Time formats:** `30m` (minute), `12h` (ghante), `7d` (din), `1y` (saal), `off` (expiry hata do)
**Examples:**
```
/expire abc123 7d
/expire abc123 12h
/expire abc123 off
```

### `/setpass` — Password lagao (Uploader ya Admin)
Link ko password-protected banata hai. Sirf wahi user (jisne upload kiya) ya admin laga sakta hai.
**Use:** `/setpass <file_id> <password>`
**Hatane ke liye:** `/setpass <file_id> off`
**Example:** `/setpass abc123 mypass123`

### `/dminem` — Saari files delete (Admin) ⚠️
**Saari files permanently delete** kar deta hai. Pehle confirm ka button aata hai ("Haan, SAB delete karo" / "Cancel"), seedha delete nahi hota. Bahut dhyan se use karna.
**Use:** `/dminem`

---

## 3. File tagging (website pe organise karne ke liye)

### `/tag` — Subject / Chapter tag (Admin)
**Use:** `/tag <file_id> <Subject>` ya `/tag <file_id> <Subject> / <Chapter>`
**Examples:**
```
/tag abc123 Physics
/tag abc123 Physics / Chapter 3 - Motion
```

### `/untag` — Tag hatao (Admin)
**Use:** `/untag <file_id>`

### `/setyear` — Year tag (Admin)
Year **1930 se 2030** ke beech hona chahiye. `0` likhne se year hat jata hai.
**Use:** `/setyear <file_id> <year>`
**Examples:** `/setyear abc123 1994` aur `/setyear abc123 0`

### `/setepisode` — Season/Episode/Part label (Admin)
**Use:** `/setepisode <file_id> <label>`
**Examples:**
```
/setepisode abc123 Season 2 Episode 5
/setepisode abc123 Part 3
```
Label khali chhodne se (`/setepisode abc123`) label hat jata hai.

---

## 4. Visitors / Access ID management

Website pe aane wale har visitor ko ek 5-digit **Access ID** milti hai. Admin use approve ya block karta hai.

### `/user` — Recent visitors ki list (Admin)
Pichhle 30 visitors ka naam, Access ID aur status (⏳ pending / ✅ approved / 🚫 blocked) dikhata hai.
**Use:** `/user`

### `/profile` — Visitor ka poora profile (Admin)
Naam, status, about, email, phone, Instagram aur Facebook (jo visitor ne bhara ho) dikhata hai.
**Use:** `/profile <access_id>`
**Example:** `/profile 48213`

### `/approve` — Visitor approve (Admin)
Uska page apne aap unlock ho jata hai.
**Use:** `/approve <access_id>`

### `/block` — Visitor block (Admin)
Wo device kisi bhi link ko access nahi kar paega.
**Use:** `/block <access_id>`

### `/unblock` — Block hatao (Admin)
**Use:** `/unblock <access_id>`

### `/reject` — Visitor ka record poora delete (Admin)
Visitor ka record hat jata hai, wo bilkul naye sire se aayega.
**Use:** `/reject <access_id>`

### `/deleteuser` — Approval + premium dono delete (Admin)
`/reject` jaisa hai, par saath mein uska **premium bhi hata deta hai**. Use dobara poora process karna padega.
**Use:** `/deleteuser <access_id>`

### `/clearpending` — Saare pending visitors saaf (Admin)
Sirf ⏳ pending wale delete hote hain. Approved aur blocked IDs safe rehte hain.
**Use:** `/clearpending`

### `/ban` — Telegram user ban (Admin)
Ye **Telegram user ID** se kaam karta hai (Access ID se nahi). Banned user bot use nahi kar sakta.
**Use:** `/ban <user_id>`
**Example:** `/ban 123456789`

### `/unban` — Ban hatao (Admin)
**Use:** `/unban <user_id>`

---

## 5. Premium aur codes

### `/premium` — Seedha premium do (Admin)
**Use:** `/premium <access_id> <days>`
**Example:** `/premium 48213 30` (30 din ka premium)

### `/unpremium` — Premium hatao (Admin)
**Use:** `/unpremium <access_id>`

### `/gencode` — Premium code banao (Admin)
Customer ko bhejne ke liye code banata hai. Customer `website/redeem` pe jakar "Have a Code?" mein daalta hai.
**Use:** `/gencode <time> [kitne_log]`
- `<time>`: sirf number = **din** (`30`), ya `90m`, `12h`, `7d`, `1y`
- `[kitne_log]`: kitne alag devices use kar sakte hain (default 1, max 100000)

**Examples:**
```
/gencode 30        → 30 din, 1 banda
/gencode 12h       → 12 ghante, 1 banda
/gencode 7d 50     → 7 din, 50 log tak
/gencode 30m 5     → 30 minute, 5 log tak
```

### `/deletecode` — Premium code delete (Admin)
**Use:** `/deletecode <code>`
**Example:** `/deletecode A1B2C3D4`

---

## 6. Website ki settings / content

### `/reply` — Site-wide announcement (Admin)
Website pe sabko ek glowing banner mein message dikhta hai.
**Use:** `/reply <message>`
**Hatane ke liye:** `/clearreply`

### `/clearreply` — Announcement hatao (Admin)
**Use:** `/clearreply`

### `/code` — Custom HTML/CSS/JS section (Admin)
Jo bhi bhejoge wo website ke sabse niche, footer ke neeche har page pe dikhega. `<script>` bhi chalta hai. Visitors ko page refresh karna padega.
**Use:** `/code <html/css/js>`
**Example:** `/code <div style="color:red">Hello</div>`
**Hatane ke liye:** `/removecode`
⚠️ Ye code seedha chalta hai, isliye sirf apna bharosemand code hi daalo.

### `/removecode` — Custom section hatao (Admin)
**Use:** `/removecode`

### `/video` — Password/approval page ka video (Admin)
Wo video jo Access ID wale page ke upar chalta hai.
**Tarike:**
- `/video` bhejo, phir **5 minute ke andar** koi video bhejo
- `/video <video ka URL>`
- `/video off` — hata do

### `/video2` — Referral welcome video (Admin)
Jo naya user kisi ki referral link se aata hai, use sabse pehle ye video dikhta hai. **Sirf video upload accept karta hai, URL nahi.**
**Steps:** `/video2` bhejo, phir 5 minute ke andar video bhejo.
**Hatane ke liye:** `/video2 off`

### `/advertise` — Watch page ka ad banner (Admin)
**Tarike:**
- `/advertise` bhejo, phir 5 minute ke andar photo ya video bhejo
- `/advertise <image ya video URL>` (`.mp4 .webm .mov .mkv .m3u8` wale links video maane jate hain)
- `/advertise off` — hata do

### `/admin` — Website pe "Admin" badge dikhao (Admin)
Sirf naam + photo ke saath showcase/credit ke liye hai. Isse koi asli bot-access nahi milta.
**Use:** `/admin <naam> <image_url>`
**Example:** `/admin Rahul https://example.com/photo.jpg`

### `/removeadmin` — Admin badge hatao (Admin)
**Use:** `/removeadmin <naam>`

### `/dashboard` — Admin dashboard ka link (Admin)
Dashboard ka private link deta hai. **Kisi ko share mat karna.**
**Use:** `/dashboard`

### `/stats` — Statistics (Admin)
Total files, users, bots, unique views aur abhi live kitne log hain, ye dikhata hai.
**Use:** `/stats`

---

## 7. Dusre channel se bulk import

### `/setsource` — Source channel set (Admin)
Tumhara wo doosra channel jahan videos/PDFs pehle se hain. Iske baad har Movie/Book Request ke saath admin ko **"🔎 Find & Upload"** button milta hai jo isi channel mein dhoondhta hai.
**Use:** `/setsource <channel_id>`
**Example:** `/setsource -1002795064458`
Channel ID `@userinfobot` ya `@RawDataBot` ko koi post forward karke mil jati hai.

### `/import` — Bulk import (Admin)
Channel ke message ID range se saari files library mein le aata hai.
**Use:** `/import <channel_id> <start_msg_id> <end_msg_id>`
**Example:** `/import -1002795064458 1 500`

**Dhyan rakho:**
- **Teeno values zaroori hain**, spaces se alag. `3000-1` ek hi value maani jati hai.
- Ek baar mein **max 50000 messages**.
- Bot us channel mein admin ya member hona chahiye.
- Ek hi status message percentage, ETA aur last msg ID ke saath update hota rehta hai.
- Har message ke beech 1.2 sec ka gap hai, to 50000 mein lagbhag 16–17 ghante lagte hain.
- Ek time pe ek hi import chal sakta hai.
- Server restart ho jaye to import ruk jata hai. Aakhri `Last msg ID` se aage `/import <channel> <last+1> <end>` chala do.

### `/cancelimport` — Chal rahe import ko roko (Admin)
Current message ke baad import ruk jata hai aur aage badhane ka ready command bata deta hai.
**Use:** `/cancelimport`

---

## 📋 BotFather ke liye ready list

BotFather mein `/setcommands` chalao, apna bot chuno, aur ye paste karo (sirf wahi rakhna jo menu mein dikhana chahte ho):

```
start - Welcome message
help - Saare commands
u - Multi-quality upload shuru
d - Multi-quality upload khatam
stats - Bot statistics
dashboard - Admin dashboard link
expire - Link ki expiry set karo
setpass - File pe password lagao
delete - File delete karo
tag - Subject/Chapter tag
untag - Tag hatao
setyear - Year tag
setepisode - Season/Episode label
user - Recent visitors list
profile - Visitor ka profile
approve - Visitor approve
block - Visitor block
unblock - Visitor unblock
reject - Visitor record delete
deleteuser - Visitor + premium delete
clearpending - Pending visitors saaf
ban - Telegram user ban
unban - Telegram user unban
premium - Premium do
unpremium - Premium hatao
gencode - Premium code banao
deletecode - Premium code delete
reply - Site announcement
clearreply - Announcement hatao
code - Custom HTML/CSS/JS section
removecode - Custom section hatao
video - Approval page video
video2 - Referral welcome video
advertise - Watch page ad banner
admin - Website pe admin badge
removeadmin - Admin badge hatao
setsource - Source channel set
import - Bulk import
cancelimport - Import roko
dminem - Saari files delete
```

> Note: BotFather command naam sirf chhote letters, digits aur underscore le sakta hai, aur description 256 characters tak. Ye list ke saare naam isme fit hote hain.

---

## ⚠️ Khatarnak commands (soch ke chalana)

| Command | Kya hota hai |
|---|---|
| `/dminem` | Saari files permanently delete |
| `/delete` | File (aur uski saari qualities) delete |
| `/deleteuser` | Visitor ka approval + premium dono delete |
| `/clearpending` | Saare pending visitors delete |
| `/code` | Seedha website pe chalne wala code |
