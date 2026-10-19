# HLD — Global Gaming Leaderboard | Proper Hinglish Classroom

**Socho** tum gaming startup ke backend architect ho. 1 crore players hain. Har match ke baad score aata hai, aur sabko top-100, apna rank aur aas-paas ke players dekhne hain. Peak 50,000 score updates/s aur 20,000 reads/s maan kar design karenge; ye assumptions hain, measured numbers nahi.

## Pehle requirements clear karo

Score cumulative ya highest? Ties ka rule? Season reset kab? Cheating kaise rokoge? Ye interviewer se clarify karo. Hum cumulative per-season score, authenticated game-server events aur eventual rank consistency choose karenge.

## Architecture kaise sochenge?

**Ab problem:** Direct Redis increment par consumer replay score double kar sakta hai. Isliye durable event ID, authoritative database aur Redis ko rebuildable read projection rakho. Kafka topic player ID se partition karo, taaki ek player ke events ordered rahen.
