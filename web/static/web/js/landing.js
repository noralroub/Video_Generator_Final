// Landing page interactive features

document.addEventListener('DOMContentLoaded', function() {
    // Smooth scroll for navigation
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });

    // Parallax effect on scroll
    const parallaxElements = document.querySelectorAll('.gradient-orb');
    window.addEventListener('scroll', function() {
        parallaxElements.forEach((element, index) => {
            const scrollPosition = window.pageYOffset;
            const speed = 0.5 + (index * 0.1);
            element.style.transform = `translateY(${scrollPosition * speed}px)`;
        });
    });

    // Add scroll animation to feature cards
    const observer = new IntersectionObserver((entries) => {
        entries.forEach((entry, index) => {
            if (entry.isIntersecting) {
                setTimeout(() => {
                    entry.target.style.animation = `fadeInUp 0.6s ease-out forwards`;
                }, index * 100);
                observer.unobserve(entry.target);
            }
        });
    }, {
        threshold: 0.1,
        rootMargin: '0px 0px -100px 0px'
    });

    document.querySelectorAll('.feature-card, .step-card').forEach(card => {
        card.style.opacity = '0';
        observer.observe(card);
    });

    // Counter animation for stats
    const statsObserver = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
            if (entry.isIntersecting) {
                animateCounters();
                statsObserver.unobserve(entry.target);
            }
        });
    });

    const impactSection = document.querySelector('.impact');
    if (impactSection) {
        statsObserver.observe(impactSection);
    }

    function animateCounters() {
        const counters = document.querySelectorAll('.counter');
        counters.forEach(counter => {
            const target = parseInt(counter.getAttribute('data-target'));
            const increment = target / 50;
            let current = 0;

            const updateCount = () => {
                current += increment;
                if (current < target) {
                    counter.textContent = Math.floor(current);
                    requestAnimationFrame(updateCount);
                } else {
                    counter.textContent = target;
                }
            };

            updateCount();
        });
    }

    // Navbar scroll effect
    const navbar = document.querySelector('.site-header');
    if (navbar) {
        window.addEventListener('scroll', function() {
            if (window.pageYOffset > 50) {
                navbar.style.boxShadow = '0 4px 20px rgba(0, 0, 0, 0.1)';
            } else {
                navbar.style.boxShadow = 'none';
            }
        });
    }

    // Button hover effects
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach(button => {
        button.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-3px)';
        });

        button.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(0)';
        });
    });

    // Gradient animation on hero
    const hero = document.querySelector('.hero');
    if (hero) {
        let angle = 0;
        setInterval(() => {
            angle += 0.5;
            const gradient = `linear-gradient(${angle}deg, #111111, #2b2b2b, #555555)`;
            // Subtle animation - don't overdo it
        }, 50);
    }
});

// Add fade in animation styles dynamically
const style = document.createElement('style');
style.textContent = `
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
`;
document.head.appendChild(style);
